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
import java.util.Arrays;
import java.util.List;

import com.google.common.base.Charsets;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to create and append to the logfile storing executed queries
 */
public class QueryRecorder
{
    private static final Logger logger = LoggerFactory.getLogger(QueryRecorder.class);
    private static final String queryLogFileName = "query.log";
    public static final File queryLog = new File(DatabaseDescriptor.getCommitLogLocation(), queryLogFileName);

    public QueryRecorder()
    {}

    /** creates the query recording log file */
    public void create() throws IOException
    {
        if (!queryLog.exists())
        {
            Files.createFile(queryLog.toPath());
            logger.info("Created query log {}", queryLog.getPath());
        }
    }

    public void append(String queryString)
    {
        try
        {
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

    public void read() throws IOException
    {
        List<String> timestampedQueries = Files.readAllLines(queryLog.toPath(), Charsets.UTF_8);

        for (String queryLogLine : timestampedQueries)
        {
            System.out.println("\n>");
            String [] timestampAndQuery = queryLogLine.split(" ", 2);
            for (String tupleFragment : timestampAndQuery)
                System.out.println(tupleFragment);
        }
    }

    public void replay() throws IOException
    {
        // todo
    }
}