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

import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.cassandra.db.monitoring.ApproximateTime;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingVersion;
import org.apache.cassandra.net.ProtocolVersion;

/**
 * A Netty inbound handler for messages coming through the DSE-specific protocol introduced with APOLLO-497.
 */
class InboundMessageHandler extends ByteToMessageDecoder
{
    private static final Logger logger = LoggerFactory.getLogger(InboundMessageHandler.class);

    private final InetAddress peer;
    /**
     * Should be DSE-specific protocol version.
     */
    private final ProtocolVersion protocolVersion;
    /**
     * Abstracts out depending directly on {@link MessagingService#receive(Message)}; this makes tests more sane
     * as they don't require nor trigger the entire message processing circus.
     */
    private final Consumer<Message<?>> messageConsumer;

    /* Mutable intermediary state used during the consumption of each single message. */

    /**
     * The size of the currently consumed message, if we're in the process of consuming a message, or -1, if we're
     * waiting for a new message to arrive.
     */
    private int currentMessageSize = -1;
    /**
     * Used for deserializing the received bytes into a {@link Message}.
     */
    private Message.Serializer serializer = null;

    InboundMessageHandler(InetAddress peer, ProtocolVersion protocolVersion)
    {
        this(peer, protocolVersion, MessagingService.instance()::receive);
    }

    InboundMessageHandler(InetAddress peer, ProtocolVersion protocolVersion, Consumer<Message<?>> messageConsumer)
    {
        assert protocolVersion.isDSE;
        this.peer = peer;
        this.protocolVersion = protocolVersion;
        this.messageConsumer = messageConsumer;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> list) throws Exception
    {
        ByteBufDataInputPlus inputPlus = new ByteBufDataInputPlus(in);
        try
        {
            // Check if we are in the process of reading a message.
            if (currentMessageSize < 0)
            {
                // We aren't and there's a new message - we should at least be able to read its serialized size.
                if (in.readableBytes() < 4)
                    return;
                // Create a serializer and deserialize the message size (in order to wait for that many bytes).
                serializer = Message.createSerializer(MessagingVersion.from(protocolVersion),
                                                      ApproximateTime.currentTimeMillis());
                currentMessageSize = serializer.readSerializedSize(inputPlus);
                if (currentMessageSize < 0)
                    throw new IOException("Invalid message serialized size header: " + currentMessageSize + " with protocol version " + protocolVersion);
            }
            if (in.readableBytes() < currentMessageSize)
                return;

            assert serializer != null;
            Message<Object> message = serializer.deserialize(inputPlus, peer);
            messageConsumer.accept(message);

            // Once a message has been fully consumed, reset the intermediary state.
            currentMessageSize = -1;
            serializer = null;
        }
        catch (Exception e)
        {
            exceptionCaught(ctx, e);
        }

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
        if (cause instanceof EOFException)
            logger.trace("eof reading from socket; closing", cause);
        else if (cause instanceof UnknownTableException)
            logger.warn("Got message from unknown table while reading from socket; closing", cause);
        else if (cause instanceof IOException)
            logger.trace("IOException reading from socket; closing", cause);
        else
            logger.warn("Unexpected exception caught in inbound channel pipeline from " + ctx.channel().remoteAddress(), cause);

        ctx.close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception
    {
        logger.debug("received channel closed message for peer {} on local addr {}", ctx.channel().remoteAddress(), ctx.channel().localAddress());
        ctx.fireChannelInactive();
    }
}
