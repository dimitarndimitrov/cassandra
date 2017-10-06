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

package org.apache.cassandra.net;

import java.net.InetAddress;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

/**
 * TODO
 */
public class OSSMessageHeader implements Message.Header
{
    public final MessagingVersion messagingVersion;
    public final int messageId;
    public final int timestampLoBits;
    public final InetAddress from;
    public final OSSVerb verb;
    public final int payloadSize;
    /**
     * Total number of parameters expected to be populated in {@link #parameters}.
     */
    public final int parameterCount;
    // TODO Should we use MessageParameters here instead?
    public final ImmutableMap<String, byte[]> parameters;

    private OSSMessageHeader(Builder builder)
    {
        this.messagingVersion = builder.messagingVersion;
        this.messageId = builder.messageId;
        this.timestampLoBits = builder.timestampLoBits;
        this.from = builder.from;
        this.verb = builder.verb;
        this.payloadSize = builder.payloadSize;
        this.parameterCount = builder.parameterCount;
        this.parameters = ImmutableMap.copyOf(builder.parameters);
    }

    public static class Builder
    {
        private final MessagingVersion messagingVersion;
        private int messageId;
        private int timestampLoBits;
        private InetAddress from;
        private OSSVerb verb;
        private int payloadSize;
        private int parameterCount;
        private Map<String, byte[]> parameters;

        public Builder(MessagingVersion messagingVersion)
        {
            this.messagingVersion = messagingVersion;
        }

        public Builder setMessageId(int messageId)
        {
            this.messageId = messageId;
            return this;
        }

        public Builder setTimestampLoBits(int timestampLoBits)
        {
            this.timestampLoBits = timestampLoBits;
            return this;
        }

        public Builder setFrom(InetAddress from)
        {
            this.from = from;
            return this;
        }

        public Builder setVerb(OSSVerb verb)
        {
            this.verb = verb;
            return this;
        }

        public Builder setPayloadSize(int payloadSize)
        {
            this.payloadSize = payloadSize;
            return this;
        }

        public Builder setParameterCount(int parameterCount)
        {
            this.parameterCount = parameterCount;
            return this;
        }

        public Builder setParameters(Map<String, byte[]> parameters)
        {
            this.parameters = parameters;
            return this;
        }

        public OSSMessageHeader build()
        {
            return new OSSMessageHeader(this);
        }
    }
}
