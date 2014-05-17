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
package org.apache.cassandra.cql3.recording;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

public class QueryQueue
{
    private byte[] queue;
    private AtomicInteger logPosition;
    private int limit;

    public QueryQueue(int logLimit)
    {
        this.limit = logLimit * 1024 * 1024;
    }

    public void initQueue()
    {
        queue = new byte[limit];
        logPosition = new AtomicInteger(0);
    }

    public int getPosition()
    {
        return logPosition.get();
    }

    public byte[] getQueue()
    {
        return queue;
    }

    public void incPositionBy(int delta)
    {
        logPosition.addAndGet(delta);
    }

    public boolean compareAndSetPos(int expected, int update)
    {
        return logPosition.compareAndSet(expected, update);
    }

    @Override
    public String toString() {
        return "QueryQueue{" +
                "queue=" + Arrays.toString(queue) +
                ", logPosition=" + logPosition.get() +
                ", limit=" + limit +
                '}';
    }
}
