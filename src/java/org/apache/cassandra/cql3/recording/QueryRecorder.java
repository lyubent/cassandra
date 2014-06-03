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

    public void allocate(String queryString)
    {
        if (queryQueue.get() == null)
            queryQueue.compareAndSet(null, new QueryQueue(logLimit));

        OpOrder.Group opGroup = opOrder.start();
        try
        {
            byte [] queryBytes = queryString.getBytes();
            int size = calcSegmentSize(queryBytes);

            while (true)
            {
                final QueryQueue q = queryQueue.get();
                int position = q.allocate(size);

                // check for room in queue first
                if (position >= 0)
                {
                    append(queryBytes, q);
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
     * @param logSegment Query to be recorded to the query log
     */
    private void append(byte [] logSegment, QueryQueue queue)
    {
        long timestamp = FBUtilities.timestampMicros();
        System.out.println("ts: " + timestamp + "  ls.lenght: " + logSegment.length);

        queue.getQueue().putLong(timestamp)
                        .putInt(logSegment.length)
                        .put(logSegment);
    }

    /**
     * Calculates size of a query segment
     * 8: long (timestamp), 4: int (query length), n: query string
     *
     * @param queryBytes query for which to calculate size
     * @return
     */
    private int calcSegmentSize(byte[] queryBytes)
    {
        return 8 + 4 + queryBytes.length;
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
