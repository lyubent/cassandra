/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.cql3.recording;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;

/**
 * Used to create and append to the logfile storing executed queries
 */
public class QueryRecorder
{
    private static final String QUERYLOG_NAME = "QueryLog";
    private static final String QUERYLOG_EXT = ".log";
    private final String QUERYLOG_DIR;
    private final int frequency;
    private final int logLimit;
    private final OpOrder opOrder = new OpOrder();
    private final ExecutorService executor = new ThreadPoolExecutor(0, 1, Long.MAX_VALUE, TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>());
    private final ConcurrentLinkedQueue<QueryQueue> recycledQueue = new ConcurrentLinkedQueue<>();
    private AtomicReference<QueryQueue> queryQueue = new AtomicReference<>();

    public QueryRecorder(int logLimit, int frequency, String queryLogDirectory)
    {
        this.frequency = frequency;
        this.logLimit = logLimit * 1024 * 1024;
        QUERYLOG_DIR = queryLogDirectory;
    }

    public void allocate(short statementType, byte[] statementId, String queryString, long threadId, List<ByteBuffer> vars)
    {
        if (queryQueue.get() == null)
            queryQueue.compareAndSet(null, new QueryQueue(logLimit));

        OpOrder.Group opGroup = opOrder.start();
        try
        {
            byte [] queryBytes = queryString.getBytes();
            int size = calcSegmentSize(statementType, queryBytes, vars, statementId);

            while (true)
            {
                final QueryQueue q = queryQueue.get();
                int position = q.allocate(size);

                // check for room in queue first
                if (position >= 0)
                {
                    append(statementType, statementId, queryBytes, threadId, q, vars);
                    break;
                }
                // recycle QueryQueues to avoid re-allocating.
                QueryQueue newQ = recycledQueue.poll();
                if (newQ == null)
                    newQ = new QueryQueue(logLimit);

                final int pos = q.getLogPosition().getAndSet(logLimit);
                if (queryQueue.compareAndSet(q, newQ))
                {
                    executor.submit(new Runnable()
                    {
                        public void run()
                        {
                            runFlush(pos, q);
                        }
                    });
                }
                else
                {
                    recycleQueue(pos, q);
                }
            }
        }
        finally
        {
            opGroup.close();
        }
    }

    /**
     * Appends nth query to the query log queue.
     *
     * @param statementType  short representing statement type
     *                       0 = a query string statement executed on the fly
     *                       1 = a prepared statement without concrete values
     *                       2 = concrete values of a prepared statement
     * @param statementId    The byte[] that is hashed by MD5
     * @param logSegment     Query to be recorded to the query log
     * @param threadId       Integer representing the id of the thread executing the query
     * @param queue          QueryQueue object storing the queries being executed
     * @param vars           ByteBuffer list of concrete values (null if statement is of type 0 or 1)
     */
    private void append(short statementType,
                        byte[] statementId,
                        byte [] logSegment,
                        long threadId,
                        QueryQueue queue,
                        List<ByteBuffer> vars)
    {
        long timestamp = FBUtilities.timestampMicros();
        // add this section for all 3 statements.
        queue.getQueue().putLong(timestamp)
                        .putShort(statementType)
                        .putInt(logSegment.length)
                        .putLong(threadId)
                        .put(logSegment);

        if (statementType != 0)
            queue.getQueue().put(statementId);

        if (statementType == 2)
        {
            int totalSize = 0;
            for (ByteBuffer bb : vars)
                totalSize += bb.limit();

            queue.getQueue().putInt(totalSize);

            for (ByteBuffer bb : vars)
                queue.getQueue().putInt(bb.limit())     // length
                                .put(bb.duplicate());   // data
        }
    }

    /**
     * Calculates size of a query segment
     * All types: 8 - long (timestamp), 2 - short (statement type), 4 - int (query string length),
     *            8 - long (thread id), n - String (query string)
     *
     * Special cases per segment type:
     * Type 0: n - query string
     * Type 1: 16 - uuid (statementId)
     * Type 2: 4 - int (total length of all concrete values), (n + 4) * n - ints + byte buffers variable
     *                  length (int for length of concrete value, and bb for the value itself)
     *
     * @param queryBytes query for which to calculate size
     * @return
     */
    private int calcSegmentSize(int statementType, byte[] queryBytes, List<ByteBuffer> vars, byte[] statementId)
    {
        int size = 8 + 2 + 4 + 8 + queryBytes.length;
        // statement id
        if (statementType != 0)
            size += statementId.length;

        // 4 bytes for total size of all vars. Size of each buffer and 4 bytes for size of each var.
        if (statementType == 2)
        {
            size += 4;
            for (ByteBuffer bb : vars)
                size += bb.limit() + 4;
        }

        return size;
    }

    public Integer getFrequency()
    {
        return frequency;
    }

    public void forceFlush()
    {
        QueryQueue q = queryQueue.get();
        runFlush(q.getPosition(), q);
    }

    private void runFlush(final int pos, QueryQueue q)
    {
        OpOrder.Barrier barrier = opOrder.newBarrier();
        barrier.issue();
        barrier.await();

        File logFile = new File(QUERYLOG_DIR, FBUtilities.timestampMicros() + QUERYLOG_NAME + QUERYLOG_EXT);
        try (FileOutputStream fos = new FileOutputStream(logFile))
        {
            fos.write(q.getQueue().array(), 0, pos);
        }
        catch (IOException iox)
        {
            throw new RuntimeException(String.format("Failed to flush query log %s", logFile.getAbsolutePath()), iox);
        }
        recycleQueue(pos, q);
    }

    private void recycleQueue(final int pos, final QueryQueue q)
    {
        if (q.getLogPosition().compareAndSet(pos, 0))
        {
            q.getQueue().clear();
            recycledQueue.add(q);
        }
    }
}
