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
    private AtomicReference<QueryQueue> queryQueue = new AtomicReference<>();
    private final OpOrder opOrder = new OpOrder();

    public QueryRecorder(int logLimit, int frequency, String queryLogDirectory)
    {
        this.frequency = frequency;
        QUERYLOG_DIR = queryLogDirectory;
        queryQueue.set(new QueryQueue(logLimit));
    }

    public void allocate(String queryString)
    {
        if (queryQueue.get().getQueue() == null)
            queryQueue.get().initQueue();

        OpOrder.Group opGroup = opOrder.start();
        try
        {
            byte [] queryBytes = queryString.getBytes();
            int size = calcSegmentSize(queryBytes);
            int position = allocate(size);
            byte [] logSegment = ByteBuffer.allocate(size)
                                           .putLong(FBUtilities.timestampMicros())
                                           .putInt(queryBytes.length)
                                           .put(queryBytes)
                                           .array();

            // check for room in queue first, if queue is full then close the op, flush the log,
            // re-initialize the queue, re-start the op and then continue with the append after
            // updating the position of the newly initialized queue.
            if (position == -1)
            {
                opGroup.close();
                runFlush();
                opGroup = opOrder.start();
                // re-allocate.
                position = allocate(size);
            }
            append(size, position, logSegment, queryQueue.get());
        }
        finally
        {
            opGroup.close();
        }
    }

    private int allocate(int size)
    {
        QueryQueue queue;
        int position;
        int length;

        while (true)
        {
            queue = queryQueue.get();
            position = queue.getPosition();
            length = queue.getQueue().length;

            if (position + size > length)
                return -1;
            else if (position + size > 0 && queue.compareAndSetPos(position, position + size))
                return position;
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

    public synchronized void runFlush()
    {
        byte[] queueToFlush = queryQueue.get().getQueue();

        int finalPos = queryQueue.get().getPosition();
        int limit = queryQueue.get().getQueue().length;
        if (queryQueue.compareAndSet(queryQueue.get(), new QueryQueue(limit)))
            queryQueue.get().initQueue();

        // todo timing out
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
