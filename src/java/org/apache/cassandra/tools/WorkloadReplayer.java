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
package org.apache.cassandra.tools;

import java.io.*;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Charsets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.cli.*;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.WrappedRunnable;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.TException;

public class WorkloadReplayer
{
    private static final Options options = new Options();
    private static final PrintStream out = System.out;
    private static final long DEFAULT_TIMEOUT = 10000000;
    private static final byte QUERYMARKER = "_".getBytes()[0];

    static
    {
        options.addOption("h",  "host",       true,  "server's host name");
        options.addOption("p",  "port",       true,  "server's thrift port");
        options.addOption("rw", "replaywait", false, "replay each query after sleeping for the time difference tween it and the previous query.");
        options.addOption("t",  "timeout",    true,  "timeout limit for period between query execution. Timeout option should only be supplied when using -rw.");
        options.addOption("h",  "help",       false, "print help information.");
    }

    public static void main(String [] args) throws ParseException, IOException, InvalidRequestException
    {
        if (args.length == 0)
        {
            printWorkloadReplayerUsage(out);
            System.exit(1);
        }

        CommandLineParser parser = new PosixParser();
        CommandLine cmd =  parser.parse(options, args);
        if (cmd.hasOption("rw"))
            printWorkloadReplayerUsage(out);

        String host = (cmd.getOptionValue("host") != null) ? cmd.getOptionValue("host") : InetAddress.getLocalHost().getHostAddress();
        int port = (cmd.getOptionValue("port") != null) ? Integer.parseInt(cmd.getOptionValue("timeout")) : DatabaseDescriptor.getRpcPort();
        long timeout = (cmd.getOptionValue("timeout") != null) ? Long.parseLong(cmd.getOptionValue("timeout")) : DEFAULT_TIMEOUT;
        boolean replayWait = cmd.hasOption("rw");
        String logPath = args[0];
        File log = new File(logPath);

        // verify we can access the query log(s)
        if (!log.exists() || !log.canRead())
            throw new InvalidRequestException(String.format("QueryLog %s doesn't exist or you don't have READ permissions.", logPath));

        File [] logPaths = log.listFiles();
        replay(replayWait, timeout, host, port, read(logPaths));
    }

    /**
     * Reads the log to be replayed
     *
     * @return List<Pair<Long, String>> A list of pairs containing L:timestamp R:queryString
     * @throws IOException
     */
    public static Iterable<Pair<Long, String>> read(File[] logPaths) throws IOException
    {
        List<Pair<Long, String>> queries = new ArrayList<>();
        for (File logPath : logPaths)
        {
            // skip files that are not query logs.
            if(!logPath.getName().contains("QueryLog"))
                continue;

            byte [] logBytes = Files.readAllBytes(logPath.getAbsoluteFile().toPath());
            int logPosition = 0;

            while (logPosition < logBytes.length)
            {
                // calculate timestamp, read 16 bytes
                byte[] timestamp = new byte[16];
                for (int i=0; i<timestamp.length; i++)
                    timestamp[i] = logBytes[logPosition + i];

                // calculate the query's lenght, read bytes until "_" is found.
                // max loop is 10 since that's the largest number of digits in an integer
                // todo rework this to avoid using the extra "dynamic length" String
                String tempQueryLength = "";
                for (int i=0; i<10; i++)
                {
                    byte currByte = logBytes[logPosition + timestamp.length + i];
                    if (currByte == QUERYMARKER)
                        break;
                    tempQueryLength += new String(new byte[]{currByte});
                }

                int queryLength = Integer.parseInt(tempQueryLength);
                byte[] queryString = new byte[queryLength];
                for (int i=0; i < queryLength; i++)
                {
                    // const to account for the "_" end of queryLenght marker.
                    int index = logPosition + 1 + i + timestamp.length + tempQueryLength.length();
                    queryString[i] = logBytes[index];
                }

                // Const 1 for going to the next for next read
                logPosition = 1 + logPosition + timestamp.length + tempQueryLength.length() + queryString.length;
                queries.add(Pair.create(Long.parseLong(new String(timestamp)), new String(queryString)));
            }
        }

        return queries;
    }

    /**
     * Executes queries against a cluster
     *
     * @throws Exception
     */
    public static void replay(final boolean rapidReplay,
                              final long timeout,
                              final String host,
                              final int port,
                              final Iterable<Pair<Long, String>> queries)
    {
        Runnable runnable = new WrappedRunnable()
        {
            public void runMayThrow() throws TException
            {
                Long previousTimestamp = 0L;
                Cassandra.Client client = createThriftClient(host, port);

                for (Pair<Long, String> query : queries)
                {
                    Long gapBetweenQueryExecutionTime = query.left - previousTimestamp;
                    // set max wait time to 10 sec
                    if(gapBetweenQueryExecutionTime > timeout)
                        gapBetweenQueryExecutionTime = timeout;
                    previousTimestamp = query.left;
                    executeQuery(rapidReplay, gapBetweenQueryExecutionTime, query.right, client);
                }
            }
        };
        runnable = new Thread(runnable, "WORKLOAD-REPLAYER");
        runnable.run();
    }

    /**
     * Execute a cql3 query via thrift.
     * @param query CQL3 query string
     * @param queryGap Gap between current and previous query
     * @throws Exception
     */
    // todo possibly swap to use java driver here.
    public static void executeQuery(boolean rapidReplay, long queryGap, String query, Cassandra.Client thriftClient)
    throws TException
    {
        out.print(String.format("Processing %s", query));
        if (rapidReplay)
        {
            out.print(String.format(" with delay of %d ms.", queryGap/1000));
            Uninterruptibles.sleepUninterruptibly(queryGap, TimeUnit.MICROSECONDS);
        }
        out.println();
        thriftClient.execute_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE, ConsistencyLevel.ANY);
    }

    public static Cassandra.Client createThriftClient(String host, int port)
    {
        try
        {
            Cassandra.Client client = new Cassandra.Client(
                                      new TBinaryProtocol(
                                      new TFramedTransportFactory().openTransport(host, port)));

            Map<String, String> credentials = new HashMap<>();
            AuthenticationRequest authenticationRequest = new AuthenticationRequest(credentials);
            client.login(authenticationRequest);
            return client;

        }
        catch (TException ex)
        {
            throw new RuntimeException("Failed to create thrift client for query replay. ", ex);
        }
    }

    private static void printWorkloadReplayerUsage(PrintStream out)
    {
        out.println("This command requires the directory of the query logs for replay!");
        out.println("Usage: workloadreplayer <querylogdir>");
        out.println("Options are:");
        out.println(String.format("Options: -t [ms] %s ", options.getOption("t").getDescription()));
        out.println(String.format("         -rw     %s ", options.getOption("rw").getDescription()));
        out.println(String.format("         -h      %s ", options.getOption("h").getDescription()));
    }
}
