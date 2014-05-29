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
package org.apache.cassandra.db;

import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertNull;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.utils.ByteBufferUtil;

public class RemoveColumnFamilyWithFlush1Test
{
    private static final String KEYSPACE1 = "RemoveColumnFamilyWithFlush1Test";
    private static final String CF_STANDARD1 = "Standard1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        List<KSMetaData> schema = new ArrayList<>();
        Class<? extends AbstractReplicationStrategy> simple = SimpleStrategy.class;

        schema.add(KSMetaData.testMetadata(KEYSPACE1,
                                           simple,
                                           KSMetaData.optsWithRF(1),
                                           SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1)));
        SchemaLoader.startGossiper();
        SchemaLoader.initSchema();
        for (KSMetaData ksm : schema)
            MigrationManager.announceNewKeyspace(ksm);
    }

    @Test
    public void testRemoveColumnFamilyWithFlush1()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF_STANDARD1);
        Mutation rm;
        DecoratedKey dk = Util.dk("key1");

        // add data
        rm = new Mutation(KEYSPACE1, dk.getKey());
        rm.add(CF_STANDARD1, Util.cellname("Column1"), ByteBufferUtil.bytes("asdf"), 0);
        rm.add(CF_STANDARD1, Util.cellname("Column2"), ByteBufferUtil.bytes("asdf"), 0);
        rm.apply();
        store.forceBlockingFlush();

        // remove
        rm = new Mutation(KEYSPACE1, dk.getKey());
        rm.delete(CF_STANDARD1, 1);
        rm.apply();

        ColumnFamily retrieved = store.getColumnFamily(QueryFilter.getIdentityFilter(dk, CF_STANDARD1, System.currentTimeMillis()));
        assert retrieved.isMarkedForDelete();
        assertNull(retrieved.getColumn(Util.cellname("Column1")));
        assertNull(Util.cloneAndRemoveDeleted(retrieved, Integer.MAX_VALUE));
    }
}
