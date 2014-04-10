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
import java.util.Arrays;

import com.google.common.base.Charsets;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.FBUtilities;

/**
 * Used to create and append to the logfile storing executed queries
 */
public class QueryRecorder
{
    private final Logger logger = LoggerFactory.getLogger(QueryRecorder.class);
    private final String queryLogFileName = "QueryLog";
    private final String queryLogExtension = ".log";
    private final int frequency;
    private volatile File queryLog;
    private final int logLimit;

    public QueryRecorder(int logLimit, int frequency, String queryLogDirectory)
    {
        this.frequency = frequency;
        this.queryLog = new File(queryLogDirectory, queryLogFileName + queryLogExtension);
        this.logLimit = logLimit;
    }

    /**
     * Creates the query log file
     *
     * @throws IOException
     */
    public File create() throws IOException
    {
        if (!queryLog.exists())
        {
            Files.createFile(queryLog.toPath());

            assert queryLog.exists();
            logger.info("Created query log {}", queryLog.getPath());
        }
        return new File(queryLog.getPath());
    }

    /**
     * Appends nth query to the query log file.
     *
     * @param queryString Query to be recorded to the query log
     */
    public synchronized void append(String queryString)
    {
        try
        {
            if (queryLog.length() > (logLimit * 1024 * 1024))
                queryLog = rotateLog(queryLog);

            byte[] queryBytes = Base64.encodeBase64(queryString.getBytes());
            Files.write(queryLog.toPath(),
                        Arrays.asList(FBUtilities.timestampMicros() + " " + new String(queryBytes)),
                        Charsets.UTF_8,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.APPEND,
                        StandardOpenOption.SYNC);
        }
        catch (IOException e)
        {
            logger.error("Failed to record query {}", queryString, e);
        }
    }

    /**
     * Rotates logs by creating a new query.log and rotating and renaming the full log to query-timestamp_of_archiving.log
     *
     * @param fullLog - The filled up log to be rotated
     * @throws IOException
     */
    public File rotateLog(File fullLog) throws IOException
    {
        Long timestamp = System.currentTimeMillis();
        File newLog = new File(fullLog.getParent(), queryLogFileName + "-" + timestamp + queryLogExtension);
        // copying, deleting and re-creating avoids race condition leading to 0 byte file creation.
        Files.copy(fullLog.toPath(), newLog.toPath());
        Files.delete(fullLog.toPath());

        // create new log
        return create();
    }

    public Integer getFrequency()
    {
        return frequency;
    }
}
