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
package org.apache.cassandra.service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;


/**
 * Example how to use an embedded cassandra service.
 *
 * Tests connect to localhost:9160 when the embedded server is running.
 *
 */
public class EmbeddedCassandraServiceTest
{
    private static EmbeddedCassandraService cassandra;
    public static final String KEYSPACE1 = "EmbeddedCassandraServiceTest";
    public static final String CF_STANDARD = "Standard1";

    /**
     * Set embedded cassandra up and spawn it in a new thread.
     *
     * @throws TTransportException
     * @throws IOException
     * @throws InterruptedException
     */
    @BeforeClass
    public static void setup() throws ConfigurationException, TTransportException, IOException, InterruptedException
    {
        SchemaLoader.startGossiper(); // need to stat gossip before the service
        SchemaLoader.initSchema();
        Schema.instance.clear(); // Schema are now written on disk and will be reloaded
        cassandra = new EmbeddedCassandraService();
        cassandra.start();
        defineSchema();
    }

    public static void defineSchema()
    {
        List<KSMetaData> schema = new ArrayList<>();
        Class<? extends AbstractReplicationStrategy> simple = SimpleStrategy.class;

        schema.add(KSMetaData.testMetadata(KEYSPACE1,
                                           simple,
                                           KSMetaData.optsWithRF(1),
                                           SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD)));
        try
        {
            for (KSMetaData ksm : schema)
                MigrationManager.announceNewKeyspace(ksm);
        }
        catch (ConfigurationException cex)
        {
            throw new RuntimeException("Error occured during schema creation.", cex);
        }
    }

    @Test
    public void testEmbeddedCassandraService()
    throws TException, CharacterCodingException
    {
        Cassandra.Client client = getClient();
        client.set_keyspace(KEYSPACE1);

        ByteBuffer key_user_id = ByteBufferUtil.bytes("1");

        long timestamp = System.currentTimeMillis();
        ColumnPath cp = new ColumnPath(CF_STANDARD);
        ColumnParent par = new ColumnParent(CF_STANDARD);
        cp.column = ByteBufferUtil.bytes("name");

        // insert
        client.insert(key_user_id,
                      par,
                      new Column(ByteBufferUtil.bytes("name")).setValue(ByteBufferUtil.bytes("Ran")).setTimestamp(timestamp),
                      ConsistencyLevel.ONE);

        // read
        ColumnOrSuperColumn got = client.get(key_user_id, cp, ConsistencyLevel.ONE);

        // assert
        assertNotNull("Got a null ColumnOrSuperColumn", got);
        assertEquals("Ran", ByteBufferUtil.string(got.getColumn().value));
    }

    /**
     * Gets a connection to the localhost client
     *
     * @return
     * @throws TTransportException
     */
    private Cassandra.Client getClient() throws TTransportException
    {
        TTransport tr = new TFramedTransport(new TSocket("localhost", DatabaseDescriptor.getRpcPort()));
        TProtocol proto = new TBinaryProtocol(tr);
        Cassandra.Client client = new Cassandra.Client(proto);
        tr.open();
        return client;
    }
}
