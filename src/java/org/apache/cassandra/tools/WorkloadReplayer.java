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

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Charsets;

import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.WrappedRunnable;
import org.apache.commons.cli.*;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

public class WorkloadReplayer
{
    private static final Options options = new Options();
    private static final PrintStream out = System.out;

    private static final int DEFAULT_THRIFT_PORT = 9160;
    private static final long DEFAULT_TIMEOUT = 10000000;
    private static final String DEFAULT_HOST = "localhost";

    static
    {
        options.addOption("h", "host", true,  String.format("hostname or IP address for replay (Default: %s)", DEFAULT_HOST));
        options.addOption("p", "port", true,  String.format("thrift port number (Default: %d)", DEFAULT_THRIFT_PORT));
        options.addOption("u", "username", true,  "Username");
        options.addOption("pw", "password", true,  "Password");
        options.addOption("w", "wait", false,  "wait the diffrence between each query");
        options.addOption("t", "timeout", true,  "Timeout limit for period between query execution. Timeout option should only be supplied when using -w");
        options.addOption("H", "help", false, "Print help information");
    }

    public static void main(String [] args) throws ParseException, IOException, InvalidRequestException
    {
        if (args.length == 0)
        {
            out.println("This command requires the directory of logs / individual logfile for replay!");
            out.println("Usage: workloadreplayer <querylog>");
            printWorkloadReplayerUsage(out);
            System.exit(1);
        }

        // todo Review this initialization
        // todo seems like lots of boilerplate
        CommandLineParser parser = new PosixParser();
        CommandLine cmd =  parser.parse(options, args);

        String hostName = (cmd.getOptionValue("host") != null) ? cmd.getOptionValue("host") : DEFAULT_HOST;
        int port = (cmd.getOptionValue("port") != null) ? Integer.parseInt(cmd.getOptionValue("port")) : DEFAULT_THRIFT_PORT;
        long timeout = (cmd.getOptionValue("timeout") != null) ? Long.parseLong(cmd.getOptionValue("timeout")) : DEFAULT_TIMEOUT;
        String username = cmd.getOptionValue("username");
        String password = cmd.getOptionValue("password");

        String logPath = args[0];
        File log = new File(logPath);

        // verify we can access the query log(s)
        if (!log.exists() || !log.canRead())
            throw new InvalidRequestException(String.format("QueryLog %s doesn't exist or you don't have READ permissions.", logPath));

        List<Pair<Long, String>> queries;
        queries = log.isDirectory() ? read(log, true) : read(log);

        try
        {
            Cassandra.Client client = createThriftClient(hostName, port, username, password);
            replay(client, queries, timeout);
        }
        catch (Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }

    public static List<Pair<Long, String>> read(File logDirectory, boolean readAllLogs) throws IOException
    {
        List<Pair<Long, String>> queries = new ArrayList<>();
        File [] logPaths;

        if (logDirectory.exists())
        {
            logPaths = logDirectory.listFiles();
            for (File logPath : logPaths)
            {
                System.out.println(logPath);
                // todo Each logfile should maybe be it's own thread
                // todo executing asych acn lead to concurrency problems, e.g. trying to insert into a table before it's created
                // todo but executing serially means that the replay wont be realistic
                queries.addAll(read(logPath));
            }
        }

        return queries;
    }

    /**
     * Reads the log to be replayed
     *
     * @return List<Pair<Long, String>> A list of pairs containing L:timestamp R:queryString
     * @throws IOException
     */
    public static List<Pair<Long, String>> read(File logPath) throws IOException
    {
        // we want the querylog path to be of node a, not this.node
        List<String> queriesFromLog = Files.readAllLines(logPath.getAbsoluteFile().toPath(), Charsets.UTF_8);
        List<Pair<Long, String>> queries = new ArrayList<>(queriesFromLog.size());

        for (String query : queriesFromLog)
        {
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
     * @throws Exception
     */
    public static void replay(final Cassandra.Client client, final List<Pair<Long, String>> queries, final long timeout)
    {
        Runnable runnable = new WrappedRunnable()
        {
            public void runMayThrow() throws Exception
            {
                Long previousTimestamp = 0L;
                for (Pair<Long, String> query : queries)
                {
                    Long gapBetweenQueryExecutionTime = query.left - previousTimestamp;
                    // todo this could be a setting in the workload replay tool, <max_wait_time> or <timeout>
                    // set max wait time to 10 sec
                    if(gapBetweenQueryExecutionTime > timeout)
                        gapBetweenQueryExecutionTime = timeout;
                    previousTimestamp = query.left;

                    // todo still debug...
                    out.println(String.format("Processing %s with a delay of %d", query.right, gapBetweenQueryExecutionTime));

                    // todo possibly swap to use java driver here.
                    executeQuery(query.right, previousTimestamp, client);
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
    private static void executeQuery(String query, long queryGap, Cassandra.Client client) throws Exception
    {
        client.execute_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE, ConsistencyLevel.ANY);
    }

    private static Cassandra.Client createThriftClient(String host, int port, String user, String passwd) throws Exception
    {
        ITransportFactory transportFactory = new TFramedTransportFactory();
        TTransport trans = transportFactory.openTransport(host, port);
        TProtocol protocol = new TBinaryProtocol(trans);
        Cassandra.Client client = new Cassandra.Client(protocol);
        if (user != null && passwd != null)
        {
            Map<String, String> credentials = new HashMap<>();
            credentials.put(IAuthenticator.USERNAME_KEY, user);
            credentials.put(IAuthenticator.PASSWORD_KEY, passwd);
            AuthenticationRequest authenticationRequest = new AuthenticationRequest(credentials);
            client.login(authenticationRequest);
        }
        return client;
    }

    private static void printWorkloadReplayerUsage(PrintStream out)
    {
        // todo...
    }
}
