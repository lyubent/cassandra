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
package org.apache.cassandra.db.index;


import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.OpOrder;

public class PerRowSecondaryIndexTest
{
    // test that when index(key) is called on a PRSI index,
    // the data to be indexed can be read using the supplied
    // key. TestIndex.index(key) simply reads the data to be
    // indexed & stashes it in a static variable for inspection
    // in the test.

    private static final String KEYSPACE = "PerRowSecondaryIndexTest";
    private static final String CF_INDEXED = "Indexed1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        List<KSMetaData> schema = new ArrayList<>();
        Class<? extends AbstractReplicationStrategy> simple = SimpleStrategy.class;

        schema.add(KSMetaData.testMetadata(KEYSPACE,
                                           simple,
                                           KSMetaData.optsWithRF(1),
                                           SchemaLoader.perRowIndexedCFMD(KEYSPACE, CF_INDEXED)));
        SchemaLoader.startGossiper();
        SchemaLoader.initSchema();
        for (KSMetaData ksm : schema)
            MigrationManager.announceNewKeyspace(ksm);
    }


    @Before
    public void clearTestStub()
    {
        PerRowSecondaryIndexTest.TestIndex.reset();
    }

    @Test
    public void testIndexInsertAndUpdate()
    {
        // create a row then test that the configured index instance was able to read the row
        Mutation rm;
        rm = new Mutation(KEYSPACE, ByteBufferUtil.bytes("k1"));
        rm.add(CF_INDEXED, Util.cellname("indexed"), ByteBufferUtil.bytes("foo"), 1);
        rm.apply();

        ColumnFamily indexedRow = PerRowSecondaryIndexTest.TestIndex.LAST_INDEXED_ROW;
        assertNotNull(indexedRow);
        assertEquals(ByteBufferUtil.bytes("foo"), indexedRow.getColumn(Util.cellname("indexed")).value());

        // update the row and verify what was indexed
        rm = new Mutation(KEYSPACE, ByteBufferUtil.bytes("k1"));
        rm.add(CF_INDEXED, Util.cellname("indexed"), ByteBufferUtil.bytes("bar"), 2);
        rm.apply();

        indexedRow = PerRowSecondaryIndexTest.TestIndex.LAST_INDEXED_ROW;
        assertNotNull(indexedRow);
        assertEquals(ByteBufferUtil.bytes("bar"), indexedRow.getColumn(Util.cellname("indexed")).value());
        assertTrue(Arrays.equals("k1".getBytes(), PerRowSecondaryIndexTest.TestIndex.LAST_INDEXED_KEY.array()));
    }

    @Test
    public void testColumnDelete()
    {
        // issue a column delete and test that the configured index instance was notified to update
        Mutation rm;
        rm = new Mutation(KEYSPACE, ByteBufferUtil.bytes("k2"));
        rm.delete(CF_INDEXED, Util.cellname("indexed"), 1);
        rm.apply();

        ColumnFamily indexedRow = PerRowSecondaryIndexTest.TestIndex.LAST_INDEXED_ROW;
        assertNotNull(indexedRow);

        for (Cell cell : indexedRow.getSortedColumns())
            assertFalse(cell.isLive());

        assertTrue(Arrays.equals("k2".getBytes(), PerRowSecondaryIndexTest.TestIndex.LAST_INDEXED_KEY.array()));
    }

    @Test
    public void testRowDelete()
    {
        // issue a row level delete and test that the configured index instance was notified to update
        Mutation rm;
        rm = new Mutation(KEYSPACE, ByteBufferUtil.bytes("k3"));
        rm.delete(CF_INDEXED, 1);
        rm.apply();

        ColumnFamily indexedRow = PerRowSecondaryIndexTest.TestIndex.LAST_INDEXED_ROW;
        assertNotNull(indexedRow);
        for (Cell cell : indexedRow.getSortedColumns())
            assertFalse(cell.isLive());

        assertTrue(Arrays.equals("k3".getBytes(), PerRowSecondaryIndexTest.TestIndex.LAST_INDEXED_KEY.array()));
    }

    public static class TestIndex extends PerRowSecondaryIndex
    {
        public static ColumnFamily LAST_INDEXED_ROW;
        public static ByteBuffer LAST_INDEXED_KEY;

        public static void reset()
        {
            LAST_INDEXED_KEY = null;
            LAST_INDEXED_ROW = null;
        }

        @Override
        public void index(ByteBuffer rowKey, ColumnFamily cf)
        {
            QueryFilter filter = QueryFilter.getIdentityFilter(DatabaseDescriptor.getPartitioner().decorateKey(rowKey),
                                                               baseCfs.getColumnFamilyName(),
                                                               System.currentTimeMillis());
            LAST_INDEXED_ROW = baseCfs.getColumnFamily(filter);
            LAST_INDEXED_KEY = rowKey;
        }

        @Override
        public void delete(DecoratedKey key, OpOrder.Group opGroup)
        {
        }

        @Override
        public void init()
        {
        }

        @Override
        public void reload()
        {
        }

        @Override
        public void validateOptions() throws ConfigurationException
        {
        }

        @Override
        public String getIndexName()
        {
            return null;
        }

        @Override
        protected SecondaryIndexSearcher createSecondaryIndexSearcher(Set<ByteBuffer> columns)
        {
            return null;
        }

        @Override
        public void forceBlockingFlush()
        {
        }

        @Override
        public ColumnFamilyStore getIndexCfs()
        {
            return null;
        }

        @Override
        public boolean indexes(CellName name)
        {
            return true;
        }

        @Override
        public void removeIndex(ByteBuffer columnName)
        {
        }

        @Override
        public void invalidate()
        {
        }

        @Override
        public void truncateBlocking(long truncatedAt)
        {
        }

        @Override
        public long estimateResultRows() {
            return 0;
        }
    }
}
