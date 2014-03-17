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
import java.nio.file.Paths;
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
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

public class WorkloadReplayer
{
    private static PrintStream out = System.out;

    public static void main(String [] args) throws  IOException, InvalidRequestException
    {
        if (args.length == 0)
        {
            out.println("This command requires the directory of logs / individual logfile and a comma separated list of Cassandra server IPs for replay!");
            out.println("Usage: workloadreplayer <querylog> <nodeipcsv>");
            System.exit(1);
        }

        String logPath = args[0];
        String [] nodeIPs = args[1].split(",");

        File log = new File(logPath);

        // verify we can access the query log(s)
        if (!log.exists() || !log.canRead())
            throw new InvalidRequestException(String.format("QueryLog %s doesn't exist or you don't have READ permissions.", logPath));

        List<Pair<Long, String>> queries;

        queries = log.isDirectory() ? read(logPath)
                : read(logPath, true);

        try
        {
            replay(queries);
        }
        catch (Exception ex)
        {
            throw new RuntimeException(ex);
        }

        // try { executeQuery(null, 0); } catch (Exception ex) { ex.printStackTrace(); }
    }

    public static List<Pair<Long, String>> read(String logDirectory, boolean readAllLogs) throws IOException
    {
        List<Pair<Long, String>> queries = new ArrayList<>();
        File logDir = new File(logDirectory);
        String [] logPaths;

        if (logDir.exists())
        {
            logPaths = new File("").list();
            for (String logPath : logPaths)
                queries.addAll(read(logPath));
        }

        return queries;
    }

    /**
     * Reads the log to be replayed
     *
     * @return List<Pair<Long, String>> A list of pairs containing L:timestamp R:queryString
     * @throws IOException
     */
    // todo Make it so we replay from <node_ip> / queryLog.toPath() to <cluster>
    public static List<Pair<Long, String>> read(String logPath) throws IOException
    {
        // we want the querylog path to be of node a, not this.node
        List<String> queriesFromLog = Files.readAllLines(Paths.get(logPath), Charsets.UTF_8);
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
    public static void replay(final List<Pair<Long, String>> queries) throws Exception
    {
        Runnable runnable = new WrappedRunnable()
        {
            public void runMayThrow() throws Exception
            {
                // todo hardcoded client, need to streamline.
                Cassandra.Client client = createThriftClient("localhost", 9160, "", "");
                Long previousTimestamp = 0L;
                for (Pair<Long, String> query : queries)
                {
                    Long gapBetweenQueryExecutionTime = query.left - previousTimestamp;
                    // todo this could be a setting in the workload replay tool, <max_wait_time> or <timeout>
                    // set max wait time to 10 sec
                    if(gapBetweenQueryExecutionTime > 10000000)
                        gapBetweenQueryExecutionTime = 10000000L;
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
}
