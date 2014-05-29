package org.apache.cassandra.db.compaction;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */
import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableScanner;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.utils.ByteBufferUtil;

@RunWith(OrderedJUnit4ClassRunner.class)
public class TTLExpiryTest
{
    public static final String KEYSPACE = "TTLExpiryTest";
    private static final String CF_STANDARD1 = "Standard1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        List<KSMetaData> schema = new ArrayList<>();
        Class<? extends AbstractReplicationStrategy> simple = SimpleStrategy.class;

        schema.add(KSMetaData.testMetadata(KEYSPACE,
                                           simple,
                                           KSMetaData.optsWithRF(1),
                                           SchemaLoader.standardCFMD(KEYSPACE, CF_STANDARD1)));
        SchemaLoader.startGossiper();
        SchemaLoader.initSchema();
        for (KSMetaData ksm : schema)
            MigrationManager.announceNewKeyspace(ksm);
    }

    @Test
    public void testSimpleExpire() throws InterruptedException
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF_STANDARD1);
        cfs.disableAutoCompaction();
        cfs.metadata.gcGraceSeconds(0);
        long timestamp = System.currentTimeMillis();
        Mutation rm = new Mutation(KEYSPACE, Util.dk("ttl").getKey());
        rm.add(CF_STANDARD1, Util.cellname("col"),
               ByteBufferUtil.EMPTY_BYTE_BUFFER,
               timestamp,
               1);
        rm.add(CF_STANDARD1, Util.cellname("col7"),
               ByteBufferUtil.EMPTY_BYTE_BUFFER,
               timestamp,
               1);

        rm.apply();
        cfs.forceBlockingFlush();

        rm = new Mutation(KEYSPACE, Util.dk("ttl").getKey());
                rm.add(CF_STANDARD1, Util.cellname("col2"),
                       ByteBufferUtil.EMPTY_BYTE_BUFFER,
                       timestamp,
                       1);
                rm.apply();
        cfs.forceBlockingFlush();
        rm = new Mutation(KEYSPACE, Util.dk("ttl").getKey());
        rm.add(CF_STANDARD1, Util.cellname("col3"),
                   ByteBufferUtil.EMPTY_BYTE_BUFFER,
                   timestamp,
                   1);
        rm.apply();
        cfs.forceBlockingFlush();
        rm = new Mutation(KEYSPACE, Util.dk("ttl").getKey());
        rm.add(CF_STANDARD1, Util.cellname("col311"),
                   ByteBufferUtil.EMPTY_BYTE_BUFFER,
                   timestamp,
                   1);
        rm.apply();

        cfs.forceBlockingFlush();
        Thread.sleep(2000); // wait for ttl to expire
        assertEquals(4, cfs.getSSTables().size());
        cfs.enableAutoCompaction(true);
        assertEquals(0, cfs.getSSTables().size());
    }

    @Test
    public void testNoExpire() throws InterruptedException
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF_STANDARD1);
        cfs.disableAutoCompaction();
        cfs.metadata.gcGraceSeconds(0);
        long timestamp = System.currentTimeMillis();
        Mutation rm = new Mutation(KEYSPACE, Util.dk("ttl").getKey());
        rm.add(CF_STANDARD1, Util.cellname("col"),
               ByteBufferUtil.EMPTY_BYTE_BUFFER,
               timestamp,
               1);
        rm.add(CF_STANDARD1, Util.cellname("col7"),
               ByteBufferUtil.EMPTY_BYTE_BUFFER,
               timestamp,
               1);

        rm.apply();
        cfs.forceBlockingFlush();

        rm = new Mutation(KEYSPACE, Util.dk("ttl").getKey());
                rm.add(CF_STANDARD1, Util.cellname("col2"),
                       ByteBufferUtil.EMPTY_BYTE_BUFFER,
                       timestamp,
                       1);
                rm.apply();
        cfs.forceBlockingFlush();
        rm = new Mutation(KEYSPACE, Util.dk("ttl").getKey());
        rm.add(CF_STANDARD1, Util.cellname("col3"),
                   ByteBufferUtil.EMPTY_BYTE_BUFFER,
                   timestamp,
                   1);
        rm.apply();
        cfs.forceBlockingFlush();
        DecoratedKey noTTLKey = Util.dk("nottl");
        rm = new Mutation(KEYSPACE, noTTLKey.getKey());
        rm.add(CF_STANDARD1, Util.cellname("col311"),
                   ByteBufferUtil.EMPTY_BYTE_BUFFER,
                   timestamp);
        rm.apply();
        cfs.forceBlockingFlush();
        Thread.sleep(2000); // wait for ttl to expire
        assertEquals(4, cfs.getSSTables().size());
        cfs.enableAutoCompaction(true);
        assertEquals(1, cfs.getSSTables().size());
        SSTableReader sstable = cfs.getSSTables().iterator().next();
        SSTableScanner scanner = sstable.getScanner(DataRange.allData(sstable.partitioner));
        assertTrue(scanner.hasNext());
        while(scanner.hasNext())
        {
            OnDiskAtomIterator iter = scanner.next();
            assertEquals(noTTLKey, iter.getKey());
        }
    }
}
