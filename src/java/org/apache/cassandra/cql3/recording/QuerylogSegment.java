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

public class QuerylogSegment
{
    long timestamp;
    String queryString;

    // todo need to store both query strings (type 0 statements) and vars
    //      of prepared statements (type 2 statements)
    enum SegmentType
    {
        QUERY_STRING,
        PREPARED_STATEMENT_VARS
    }

    public QuerylogSegment(long timestamp, byte[] queryString)
    {
        this.timestamp = timestamp;
        this.queryString = new String(queryString);
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    public String getQueryString()
    {
        return queryString;
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
