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
package org.apache.cassandra.repair;

import java.util.Collection;
import java.util.UUID;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.SSTableReader;

import com.google.common.collect.Iterables;

/**
 * Servers as a store for the sstables that have been validated for a repair session
 * in CompactionManager#doValidationCompaction. This session is created on the
 * repair coordinator and all other nodes involved in the repair.
 */
public class AnticompactionSession
{
    /** sstable(s) to anticompact*/
    public final Collection<SSTableReader> validatedForRepair;
    public final ColumnFamilyStore cfs;
    private final UUID id;

    public AnticompactionSession(UUID sessionId, Collection<SSTableReader> sstables, ColumnFamilyStore cfs)
    {
        id = sessionId;
        validatedForRepair = sstables;
        this.cfs = cfs;
        lockTables();
    }

    private void lockTables()
    {
        if (!Iterables.isEmpty(validatedForRepair))
            cfs.getDataTracker().markCompacting(validatedForRepair);
    }

    public void unlockTables()
    {
        if (!Iterables.isEmpty(validatedForRepair))
            cfs.getDataTracker().unmarkCompacting(validatedForRepair);
    }
}