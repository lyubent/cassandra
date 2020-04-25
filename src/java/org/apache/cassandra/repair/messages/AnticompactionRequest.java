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
package org.apache.cassandra.repair.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.repair.RepairJobDesc;

public class AnticompactionRequest extends RepairMessage
{
    public static MessageSerializer serializer = new AnticompactionRequestSerializer();
    public final Collection<Range<Token>> ranges;

    public AnticompactionRequest(RepairJobDesc desc, Collection<Range<Token>> ranges)
    {
        super(Type.ANTICOMPACTION_REQUEST, desc);

        this.ranges = ranges;
    }

    public static class AnticompactionRequestSerializer implements MessageSerializer<AnticompactionRequest>
    {
        public void serialize(AnticompactionRequest message, DataOutput out, int version) throws IOException
        {
            RepairJobDesc.serializer.serialize(message.desc, out, version);
            out.writeInt(message.ranges.size());
            for (Range<Token> range : message.ranges)
                AbstractBounds.serializer.serialize(range, out, version);
        }

        public AnticompactionRequest deserialize(DataInput in, int version) throws IOException
        {
            RepairJobDesc desc = RepairJobDesc.serializer.deserialize(in, version);
            int rangesCount = in.readInt();
            List<Range<Token>> ranges = new ArrayList<>(rangesCount);
            for (int i = 0; i < rangesCount; ++i)
                ranges.add((Range<Token>) AbstractBounds.serializer.deserialize(in, version).toTokenBounds());
            return new AnticompactionRequest(desc, ranges);
        }

        public long serializedSize(AnticompactionRequest message, int version)
        {
            long size = RepairJobDesc.serializer.serializedSize(message.desc, version);
            size += TypeSizes.NATIVE.sizeof(message.ranges.size());
            for (Range<Token> range : message.ranges)
                size += AbstractBounds.serializer.serializedSize(range, version);

            return size;
        }
    }
}