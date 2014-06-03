/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.tools;

import java.io.File;
import java.net.InetAddress;

import com.google.common.io.Files;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.ThriftServer;

public class WorkloadReplayTest extends SchemaLoader
{
    private static final File LOGLOCATION = Files.createTempDir();
    private static ThriftServer thriftServer;
    private static final String countQuery = "SELECT count(*) FROM \"Keyspace1\".\"StandardReplay\"";

    @Before
    public void setUp() throws Exception
    {
        StorageService.instance.initServer(0);
        if (thriftServer == null || ! thriftServer.isRunning())
        {
            thriftServer = new ThriftServer(InetAddress.getLocalHost(), 9170, 0);
            thriftServer.start();
        }
    }

    @Test
    public void testReplay1() throws Exception
    {
        // enable query recording
        StorageService.instance.enableQueryRecording(1, 1, LOGLOCATION.toString());

        // create replay cf
        QueryProcessor.process("CREATE TABLE IF NOT EXISTS \"Keyspace1\".\"StandardReplay\" (id timeuuid PRIMARY KEY)", ConsistencyLevel.ONE);

        // insert 100 columns
        for (int i = 0; i<100; i++)
        {
            String queryString = "INSERT INTO \"Keyspace1\".\"StandardReplay\" (id) VALUES (now())";
            QueryProcessor.instance.prepare(queryString, QueryState.forInternalCalls());
            QueryProcessor.instance.process(queryString, ConsistencyLevel.ONE);
        }

        // verify insert
        UntypedResultSet insertResult = QueryProcessor.processInternal(countQuery);
        assertEquals(100, insertResult.one().getLong("count"));

        // stop recording and clear the cf
        StorageService.instance.disableQueryRecording();
        ColumnFamilyStore cfs = Keyspace.open("Keyspace1").getColumnFamilyStore("StandardReplay");
        cfs.truncateBlocking();
        // verify truncation
        UntypedResultSet truncateResult = QueryProcessor.processInternal(countQuery);
        assertEquals(0, truncateResult.one().getLong("count"));

        // read the query log
        String host = InetAddress.getLocalHost().getHostAddress();
        int port = 9170;

        // replay without timeout
        for (File logLocation : LOGLOCATION.listFiles())
            WorkloadReplayer.replay(false, 1000000, host, port, WorkloadReplayer.read(logLocation));
        UntypedResultSet replayResult = QueryProcessor.processInternal(countQuery);
        assertEquals(100, replayResult.one().getLong("count"));
        // replay with timeout of 1s and with delay simulation.
        for (File logLocation : LOGLOCATION.listFiles())
            WorkloadReplayer.replay(true, 1000000, host, port, WorkloadReplayer.read(logLocation));
        UntypedResultSet replayResultWithDelay = QueryProcessor.processInternal(countQuery);
        assertEquals(200, replayResultWithDelay.one().getLong("count"));
    }

    @After
    public void tearDown() throws RequestExecutionException
    {
        // remove the test log
        for (File log : LOGLOCATION.listFiles())
            if (log.toString().contains("QueryLog"))
                assertTrue(log.delete());

        QueryProcessor.process("DROP COLUMNFAMILY \"Keyspace1\".\"StandardReplay\"", ConsistencyLevel.ONE);
    }
}
