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
package org.apache.cassandra.tracing;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import org.slf4j.helpers.MessageFormatter;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ArrayBackedSortedColumns;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.WrappedRunnable;

/**
 * ThreadLocal state for a tracing session. The presence of an instance of this class as a ThreadLocal denotes that an
 * operation is being traced.
 */
public class TraceState
{
    public final UUID sessionId;
    public final InetAddress coordinator;
    public final Stopwatch watch;
    public final ByteBuffer sessionIdBytes;
    public final long traceType;
    public final int ttl;

    public Object userData;
    public long notifyTypes;

    private boolean done;
    private int counter;
    private Object counterLock;

    public TraceState(InetAddress coordinator, UUID sessionId)
    {
        this(coordinator, sessionId, Tracing.TRACETYPE_DEFAULT);
    }

    public TraceState(InetAddress coordinator, UUID sessionId, long traceType)
    {
        this(coordinator, sessionId, traceType, Tracing.getTTL(traceType));
    }

    public TraceState(InetAddress coordinator, UUID sessionId, long traceType, int ttl)
    {
        assert coordinator != null;
        assert sessionId != null;
        assert Tracing.isValidTraceType(traceType);

        this.coordinator = coordinator;
        this.sessionId = sessionId;
        sessionIdBytes = ByteBufferUtil.bytes(sessionId);
        this.traceType = traceType;
        this.ttl = ttl;
        watch = Stopwatch.createStarted();

        done = false;
        counter = 0;
        counterLock = new Object();
    }

    public int elapsed()
    {
        long elapsed = watch.elapsed(TimeUnit.MICROSECONDS);
        return elapsed < Integer.MAX_VALUE ? (int) elapsed : Integer.MAX_VALUE;
    }

    public void stop()
    {
        done = true;
        notifyActivity();
    }

    public int waitActivity(int oldCounter, long timeout)
    {
        if (done)
            return -1;

        try
        {
            synchronized (counterLock)
            {
                if (oldCounter == -1)
                    oldCounter = counter;

                long cur, end = System.currentTimeMillis() + timeout;
                while (oldCounter == counter && (cur = System.currentTimeMillis()) < end)
                    counterLock.wait(end - cur);
                return done ? -1 : counter;
            }
        }
        catch (InterruptedException e)
        {
            return 0;
        }
    }

    private void notifyActivity()
    {
        synchronized (counterLock)
        {
            counter = (counter + 1) & Integer.MAX_VALUE;
            counterLock.notifyAll();
        }
    }

    public void trace(String format, Object arg)
    {
        trace(MessageFormatter.format(format, arg).getMessage());
    }

    public void trace(String format, Object arg1, Object arg2)
    {
        trace(MessageFormatter.format(format, arg1, arg2).getMessage());
    }

    public void trace(String format, Object[] args)
    {
        trace(MessageFormatter.arrayFormat(format, args).getMessage());
    }

    public void trace(String message)
    {
        trace(Tracing.TRACETYPE_DEFAULT, message);
    }

    public void trace(long traceType, String message)
    {
        if ((this.traceType & traceType) == 0)
            return;
        if ((this.notifyTypes & traceType) != 0)
            notifyActivity();

        TraceState.trace(sessionIdBytes, message, elapsed(), this.traceType, ttl, userData);
    }

    public static void trace(final ByteBuffer sessionIdBytes, final String message, final int elapsed, final long traceType, final int ttl, final Object userData)
    {
        final ByteBuffer eventId = ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes());
        final String threadName = Thread.currentThread().getName();
        final String command = Tracing.getCommandName(traceType);

        if (userData != null && traceType == Tracing.TRACETYPE_REPAIR)
            StorageService.instance.sendNotification("repair", message, userData);

        StageManager.getStage(Stage.TRACING).execute(new WrappedRunnable()
        {
            public void runMayThrow()
            {
                CFMetaData cfMeta = CFMetaData.TraceEventsCf;
                ColumnFamily cf = ArrayBackedSortedColumns.factory.create(cfMeta);
                Tracing.addColumn(cf, Tracing.buildName(cfMeta, eventId, ByteBufferUtil.bytes("activity")), message, ttl);
                Tracing.addColumn(cf, Tracing.buildName(cfMeta, eventId, ByteBufferUtil.bytes("source")), FBUtilities.getBroadcastAddress(), ttl);
                if (elapsed >= 0)
                    Tracing.addColumn(cf, Tracing.buildName(cfMeta, eventId, ByteBufferUtil.bytes("source_elapsed")), elapsed, ttl);
                Tracing.addColumn(cf, Tracing.buildName(cfMeta, eventId, ByteBufferUtil.bytes("thread")), threadName, ttl);
                Tracing.addColumn(cf, Tracing.buildName(cfMeta, eventId, ByteBufferUtil.bytes("command")), command, ttl);
                Tracing.mutateWithCatch(new Mutation(Tracing.TRACE_KS, sessionIdBytes, cf));
            }
        });
    }
}
