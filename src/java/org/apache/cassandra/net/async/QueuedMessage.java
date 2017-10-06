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

package org.apache.cassandra.net.async;

import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.net.Message;
import org.apache.cassandra.utils.CoalescingStrategies;

/**
 *  A wrapper for outbound messages. All messages will be retried once.
 */
public class QueuedMessage implements CoalescingStrategies.Coalescable
{
    public final Message<?> message;
    public final int id;
    public final long timestampNanos;
    private final boolean retryable;

    public QueuedMessage(Message<?> message, int id)
    {
        this(message, id, System.nanoTime(), true);
    }

    @VisibleForTesting
    public QueuedMessage(Message<?> message, int id, long timestampNanos, boolean retryable)
    {
        this.message = message;
        this.id = id;
        this.timestampNanos = timestampNanos;
        this.retryable = retryable;
    }

    /** don't drop a non-droppable message just because it's timestamp is expired */
    public boolean isTimedOut()
    {
        // TODO Figure out if this should be fully replaced by (the currently non-public) Message.isTimedOut(long).
        return !message.verb().isOneWay() && timestampNanos < System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(message.timeoutMillis());
    }

    public boolean shouldRetry()
    {
        return retryable;
    }

    public QueuedMessage createRetry()
    {
        return new QueuedMessage(message, id, System.nanoTime(), false);
    }

    public long timestampNanos()
    {
        return timestampNanos;
    }
}
