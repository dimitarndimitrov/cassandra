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
 * An immutable structure, representing a message header in the OSS messaging protocol.
 */
public class OSSMessageHeader implements Message.Header
{
    /**
     * The messaging protocol version used for this message.
     */
    // TODO Do we really need that?
    public final MessagingVersion messagingVersion;
    /**
     * The message ID.
     */
    public final int messageId;
    /**
     * The lower bits of the message millisecond timestamp. We assume that they will suffice, and if the
     * message is received in less than roughly a month, indeed the higher bits can be inferred from the
     * timestamp at the receiver.
     */
    public final int timestampLoBits;
    /**
     * The origin address of this message.
     */
    public final InetAddress from;
    /**
     * The OSS verb of this message.
     *
     * @see OSSVerb
     */
    public final OSSVerb verb;
    /**
     * The message payload size (excluding the header) in bytes.
     */
    public final int payloadSize;
    /**
     * The total number of parameters expected to be populated in {@link #parameters}.
     */
    public final int parameterCount;
    /**
     * The map of raw, byte array parameters.
     */
    // TODO Should we use MessageParameters here instead?
    public final ImmutableMap<String, byte[]> parameters;

    /**
     * Creates a new message header with the given parameters
     */
    public static OSSMessageHeader from(MessagingVersion messagingVersion,
                                        int messageId,
                                        int timestampLoBits,
                                        InetAddress from,
                                        OSSVerb verb,
                                        int payloadSize,
                                        int parameterCount,
                                        Map<String, byte[]> parameters)
    {
        OSSMessageHeader header = new OSSMessageHeader(messagingVersion,
                                                       messageId,
                                                       timestampLoBits,
                                                       from,
                                                       verb,
                                                       payloadSize,
                                                       parameterCount,
                                                       parameters);
        return header;
    }

    private OSSMessageHeader(MessagingVersion messagingVersion,
                            int messageId,
                            int timestampLoBits,
                            InetAddress from,
                            OSSVerb verb,
                            int payloadSize,
                            int parameterCount,
                            Map<String, byte[]> parameters)
    {
        this.messagingVersion = messagingVersion;
        this.messageId = messageId;
        this.timestampLoBits = timestampLoBits;
        this.from = from;
        this.verb = verb;
        this.payloadSize = payloadSize;
        this.parameterCount = parameterCount;
        this.parameters = ImmutableMap.copyOf(parameters);
    }
    
}
