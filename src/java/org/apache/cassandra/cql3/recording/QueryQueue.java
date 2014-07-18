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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

public class QueryQueue
{
    private final ByteBuffer queue;
    private final AtomicInteger logPosition;

    public QueryQueue(int logLimit)
    {
        queue = ByteBuffer.allocate(logLimit);
        logPosition = new AtomicInteger(0);
    }

    public int getPosition()
    {
        return logPosition.get();
    }

    public AtomicInteger getLogPosition()
    {
        return logPosition;
    }

    public ByteBuffer getQueue()
    {
        return queue;
    }

    public int allocate(int size)
    {
        int position = logPosition.get();
        int length = queue.limit();
        if (position + size < length && logPosition.compareAndSet(position, position + size))
            return position;
        return -1;
    }

    @Override
    public String toString() {
        return "QueryQueue{" +
                "queue=" + Arrays.toString(queue.array()) +
                ", logPosition=" + logPosition.get() +
                '}';
    }
}
