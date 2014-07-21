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

import java.util.List;
import java.nio.ByteBuffer;

import org.apache.cassandra.utils.MD5Digest;

public class QuerylogSegment
{
    private final long timestamp;
    private final String queryString;
    public final SegmentType queryType;
    private final List<ByteBuffer> vars;
    private final MD5Digest statementId;

    public static enum SegmentType
    {
        QUERY_STRING,
        PREPARED_STATEMENT,
        PREPARED_STATEMENT_VARS
    }

    public QuerylogSegment(long timestamp, MD5Digest statementId, byte[] queryString, SegmentType queryType)
    {
        this.timestamp = timestamp;
        this.queryString = new String(queryString);
        this.queryType = queryType;
        this.vars = null;
        this.statementId = statementId;
    }

    public QuerylogSegment(long timestamp, MD5Digest statementId, List<ByteBuffer> vars, SegmentType queryType)
    {
        this.timestamp = timestamp;
        this.queryString = null;
        this.queryType = queryType;
        this.vars = vars;
        this.statementId = statementId;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    public String getQueryString()
    {
        return queryString;
    }

    public MD5Digest getStatementId()
    {
        return statementId;
    }

    public List<ByteBuffer> getValues()
    {
        return vars;
    }

    @Override
    public String toString()
    {
        return "QuerylogSegment{" +
                "timestamp=" + timestamp +
                ", queryString='" + queryString + '\'' +
                '}';
    }
}
