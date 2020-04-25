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
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.FBUtilities;

/**
 * Used to create and append to the logfile storing executed queries
 */
public class QueryRecorder
{
    public static String EOLMARKER = "_";
    private final String queryLogFileName = "QueryLog";
    private final String queryLogExtension = ".log";
    private final String queryLogDirectory;
    private final int frequency;
    private final int logLimit;
    private volatile byte[] queryQue;
    private volatile int logPosition;

    private final Logger logger = LoggerFactory.getLogger(QueryRecorder.class);

    public QueryRecorder(int logLimit, int frequency, String queryLogDirectory)
    {
        this.frequency = frequency;
        this.queryLogDirectory = queryLogDirectory;
        this.logLimit = logLimit * 1024 * 1024;
        logPosition = 0;
    }

    /**
     * Appends nth query to the query log file.
     *
     * @param queryString Query to be recorded to the query log
     */
    public synchronized void append(String queryString)
    {
        // lazy init. the query queue
        if (queryQue == null)
            queryQue = new byte[logLimit];


        int qSCounterLenght = String.valueOf(queryString.length()).length();
        int newEntryLenght = queryString.length() + 16 + qSCounterLenght + EOLMARKER.length();
        byte[] queryBytes = new String(FBUtilities.timestampMicros() + "" + queryString.length() + EOLMARKER + queryString).getBytes();

        if (newEntryLenght + logPosition < queryQue.length)
        {
            for (int i = 0; i < newEntryLenght; i++)
            {
                queryQue[logPosition] = queryBytes[i];
                logPosition++;
            }
        }
        // the log is full, flush it to disk
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
        try
        {
            // todo do dense flush only if the query que is mostly empty (as determened by how far the pointer is)
            // minimise wasted disc space by only flushing as far as the pointer.
            byte[] denseLog = new byte[logPosition];
            for (int i = 0; i < logPosition; i++)
                denseLog[i] = queryQue[i];

            File queryLog = new File(queryLogDirectory, FBUtilities.timestampMicros() + queryLogFileName + queryLogExtension);
            Files.write(queryLog.toPath(),
                        denseLog,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.SYNC);

            logPosition = 0;
            queryQue = null;
        }
        catch (IOException ioe)
        {
            logger.error("Failed to create query log {}", ioe);
        }
    }
}
