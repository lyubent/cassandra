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
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.WrappedRunnable;

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

    /**
     * Creates the query log file
     *
     * @throws IOException
     */
    public void create() throws IOException
    {
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
     * Reads the log to be replayed
     *
     * @return List<Pair<Long, String>> A list of pairs containing L:timestamp R:queryString
     * @throws IOException
     */
    // todo Make it so we replay from <node_ip> / queryLog.toPath() to <cluster>
    public List<Pair<Long, String>> read() throws IOException
    {
        // we want the querylog path to be of node a, not this.node
        List<String> queriesFromLog = Files.readAllLines(queryLog.toPath(), Charsets.UTF_8);
        List<Pair<Long, String>> queries = new ArrayList<>(queriesFromLog.size());

        for (String query : queriesFromLog)
        {
            // todo needs to be read line by line or fragmented to n lines
            // todo reading the whole thing = bad memory usage

            // Split the log line by the first space i.e. split the query and the timestamp
            String [] timestampAndQuery = query.split(" ", 2);
            // We are expecting each line to contain a queryString and a timestamp
            assert timestampAndQuery.length == 2;
            queries.add(Pair.create(Long.parseLong(timestampAndQuery[0]), timestampAndQuery[1]));
        }

        return queries;
    }

    /**
     * Executes queries against a cluster
     *
     * @param queries A list of pairs with left being the right being the timestamp and queryString
     * @throws IOException
     */
    public void replay(final List<Pair<Long, String>> queries) throws IOException
    {
        Runnable runnable = new WrappedRunnable()
        {
            public void runMayThrow() throws IOException
            {
                Long previousTimestamp = 0L;
                for (Pair<Long, String> query : queries)
                {
                    Long gapBetweenQueryExecutionTime = query.left - previousTimestamp;
                    // todo this could be a setting in the workload replay tool, <max_wait_time> or <timeout>
                    // set max wait time to 10 sec
                    if(gapBetweenQueryExecutionTime > 10000000)
                        gapBetweenQueryExecutionTime = 10000000L;
                    previousTimestamp = query.left;

                    logger.debug("Processing {} with a delay of {}", query.right, gapBetweenQueryExecutionTime);
                    QueryProcessor.processInternal(query.right);
                }
            }
        };
        runnable = new Thread(runnable, "WORKLOAD-REPLAYER");
        runnable.run();
    }

    public void run() throws IOException
    {
        List<Pair<Long, String>> queries = read();
        replay(queries);
    }
}
