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
package org.apache.cassandra.cql3;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.utils.FBUtilities;

/**
 * Used to create and append to the logfile storing executed queries
 */
public class QueryRecorder
{
    private final String queryLogFileName = "QueryLog";
    private final String queryLogExtension = ".log";
    private final String queryLogDirectory;
    private final int frequency;
    private AtomicReference<QueryQueue> queryQueue = new AtomicReference<>();

    public QueryRecorder(int logLimit, int frequency, String queryLogDirectory)
    {
        this.frequency = frequency;
        this.queryLogDirectory = queryLogDirectory;
        queryQueue.set(new QueryQueue(logLimit));
    }

    /**
     * Appends nth query to the query log file.
     *
     * @param queryString Query to be recorded to the query log
     */
    public synchronized void append(String queryString)
    {
        // lazy init. the query buffer
        if (queryQueue.get().queue == null)
            queryQueue.get().initQueue();

        // 8: long (timestamp), 4: int (query length), n: query string
        int size = 8 + 4 + queryString.length();
        byte [] queryBytes = ByteBuffer.allocate(size)
                                       .putLong(FBUtilities.timestampMicros())
                                       .putInt(queryString.length())
                                       .put(queryString.getBytes())
                                       .array();

        // check queue has enough room to add current query
        if (size + queryQueue.get().getPosition() < queryQueue.get().queue.length)
        {
            System.arraycopy(queryBytes, 0, queryQueue.get().queue, queryQueue.get().getPosition(), queryBytes.length);
            queryQueue.get().incPositionBy(size);
        }
        else
        {
            runFlush();
        }
    }

    public Integer getFrequency()
    {
        return frequency;
    }

    public void runFlush()
    {
        File logFile = new File(queryLogDirectory, FBUtilities.timestampMicros() + queryLogFileName + queryLogExtension);
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(logFile)))
        {
            dos.write(queryQueue.get().queue, 0, queryQueue.get().getPosition());
        }
        catch (IOException iox)
        {
            throw new RuntimeException(String.format("Failed to flush query log %s", logFile.getAbsolutePath()), iox);
        }

        // TODO create a new queryQueue instead of using this one, using compare and set perhaps.
        queryQueue.get().initQueue();
        queryQueue.get().logPosition.set(0);
    }

    public class QueryQueue
    {
        private byte[] queue;
        private AtomicInteger logPosition;
        private int limit;

        public QueryQueue(int logLimit)
        {
            this.limit = logLimit * 1024 * 1024;
            logPosition = new AtomicInteger(0);
        }

        public void initQueue()
        {
            queue = new byte[limit];
        }

        public int getPosition()
        {
            return logPosition.get();
        }

        public void incPositionBy(int delta)
        {
            logPosition.addAndGet(delta);
        }
    }
}
