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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.base.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.WrappedRunnable;

/**
 * Used to create and append to the logfile storing executed queries
 */
public class QueryRecorder
{
    private final Logger logger = LoggerFactory.getLogger(QueryRecorder.class);
    private final String queryLogFileName = "QueryLog";
    private final String queryLogExtension = ".log";
    private final Integer frequency;
    private final File queryLogDirectory;
    private final File queryLog;

    public QueryRecorder(String queryLogDirectory, Integer frequency)
    {
        this.frequency = frequency;
        this.queryLogDirectory = new File(queryLogDirectory);
        this.queryLog = new File(queryLogDirectory, queryLogFileName + queryLogExtension);
    }

    /**
     * Creates the query log file
     *
     * @throws IOException
     */
    public void create() throws IOException
    {
        System.out.println(queryLogDirectory);
        System.out.println(queryLog);

        if (!queryLogDirectory.isDirectory())
        {
            System.out.println("made dir");
            Files.createDirectory(queryLogDirectory.toPath());
            logger.info("Created query log directory {}", queryLogDirectory.getPath());
        }
        if (!queryLog.exists())
        {
            Files.createFile(queryLog.toPath());
            logger.info("Created query log {}", queryLog.getPath());
        }
    }

    /**
     * Appends nth query to the query log file.
     *
     * @param queryString Query to be recorded to the query log
     */
    // todo, append until file is 4MB and then rotate the logs.
    public void append(String queryString)
    {
        try
        {

            // todo seems silly to check the log size every append, probably want some kind of counter that checks
            // todo every nth query with the magic number being 5k (37859 appends = 4 MB with a 19 char statement)
            // todo the 4MB limit might be a setting in cassandra.yaml / via jmx function
            // limit log to 4MB
            if(queryLog.length() > (4 * 1024 * 1024))
                rotateLog(queryLog);

            Files.write(queryLog.toPath(),
                        Arrays.asList(FBUtilities.timestampMicros() + " " + queryString),
                        Charsets.UTF_8,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.APPEND,
                        StandardOpenOption.DSYNC);
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
    public void rotateLog(File fullLog) throws IOException
    {
        // rename the old log
        fullLog.renameTo(new File(DatabaseDescriptor.getCommitLogLocation(),
        queryLogFileName + "-" + System.currentTimeMillis() + queryLogExtension));
        // create new log
        create();
    }

    public Integer getFrequency()
    {
        return frequency;
    }
}
