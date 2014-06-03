package org.apache.cassandra.service;
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
import java.util.List;
import java.util.LinkedList;

import org.junit.BeforeClass;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.CellNames;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.db.ConsistencyLevel;

public class AntiEntropyServiceCounterTest extends AntiEntropyServiceTestAbstract
{
    private static final String KEYSPACE5 = "Keyspace5";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.createKeyspace(KEYSPACE5,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(2),
                                    SchemaLoader.standardCFMD(KEYSPACE5, "Counter1"));
    }

    public void init()
    {
        keyspaceName = KEYSPACE5;
        cfname    = "Counter1";
    }

    public List<IMutation> getWriteData()
    {
        List<IMutation> rms = new LinkedList<IMutation>();
        Mutation rm = new Mutation(keyspaceName, ByteBufferUtil.bytes("key1"));
        rm.addCounter(cfname, CellNames.simpleDense(ByteBufferUtil.bytes("Column1")), 42);
        rms.add(new CounterMutation(rm, ConsistencyLevel.ONE));
        return rms;
    }
}
