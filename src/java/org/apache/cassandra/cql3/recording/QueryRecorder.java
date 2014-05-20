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
    private final ConcurrentLinkedQueue<QueryQueue> failedFlushQueue = new ConcurrentLinkedQueue<>();
    private AtomicReference<QueryQueue> queryQueue = new AtomicReference<>();

    public QueryRecorder(int logLimit, int frequency, String queryLogDirectory)
    {
        this.frequency = frequency;
        this.logLimit = logLimit;
        QUERYLOG_DIR = queryLogDirectory;
    }

    public void allocate(String queryString)
    {
        // initialise first queue, ensure CAS succeeded
        if (queryQueue.get() == null)
            assert queryQueue.compareAndSet(null, new QueryQueue(logLimit));

        OpOrder.Group opGroup = opOrder.start();
        try
        {

            byte [] queryBytes = queryString.getBytes();
            int size = calcSegmentSize(queryBytes);
            byte [] logSegment = ByteBuffer.allocate(size)
                                           .putLong(FBUtilities.timestampMicros())
                                           .putInt(queryBytes.length)
                                           .put(queryBytes)
                                           .array();
            QueryQueue q;
            while (true)
            {
                q = queryQueue.get();
                int position = q.allocate(size);
                // check for room in queue first, if queue is full then CAS the queue,
                // upon success flush the log in it's own thread and repeat the loop to
                // re-allocate the byte[] to be appended to the newly set queue.
                if (position == -1)
                {
                    final int limit = q.getQueue().length;
                    final byte[] queueToFlush = q.getQueue();
                    final int pos = q.getPosition();

                    if (queryQueue.compareAndSet(q, new QueryQueue(limit)))
                    {
                        executor.submit(new Runnable()
                        {
                            public void run()
                            {
                                runFlush(pos, queueToFlush);
                            }
                        });
                    }
                    else
                    {
                        failedFlushQueue.add(q);
                    }

                    // re-allocate.
                    continue;
                }

                append(size, position, logSegment, q);
                break;
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
    private void append(int size, int position, byte [] logSegment, QueryQueue queue)
    {
        System.arraycopy(logSegment, 0, queue.getQueue(), position, size);
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

    public void runFlush()
    {
        QueryQueue q = queryQueue.get();
        runFlush(q.getPosition(), q.getQueue());
    }

    public void runFlush(final int finalPos, final byte[] queueToFlush)
    {
        OpOrder.Barrier barrier = opOrder.newBarrier();
        barrier.issue();
        barrier.await();

        File logFile = new File(QUERYLOG_DIR, FBUtilities.timestampMicros() + QUERYLOG_NAME + QUERYLOG_EXT);
        try (FileOutputStream fos = new FileOutputStream(logFile))
        {
            fos.write(queueToFlush, 0, finalPos);
        }
        catch (IOException iox)
        {
            throw new RuntimeException(String.format("Failed to flush query log %s", logFile.getAbsolutePath()), iox);
        }
    }
}
