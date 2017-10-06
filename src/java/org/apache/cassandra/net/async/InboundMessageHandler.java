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
 * TODO
 */
class InboundMessageHandler extends ByteToMessageDecoder
{
    public static final Logger logger = LoggerFactory.getLogger(InboundMessageHandler.class);

    private final InetAddress peer;
    private final ProtocolVersion protocolVersion;
    /**
     * Abstracts out depending directly on {@link MessagingService#receive(Message)}; this makes tests more sane
     * as they don't require nor trigger the entire message processing circus.
     */
    private final Consumer<Message<?>> messageConsumer;

    /**
     * TODO
     */
    private int currentFrameSize = -1;

    InboundMessageHandler(InetAddress peer, ProtocolVersion protocolVersion)
    {
        this(peer, protocolVersion, MessagingService.instance()::receive);
    }

    InboundMessageHandler(InetAddress peer, ProtocolVersion protocolVersion, Consumer<Message<?>> messageConsumer)
    {
        ProtocolVersion minVersion = MessagingVersion.DSE_60.protocolVersion();
        assert protocolVersion.isDSE && protocolVersion.compareTo(minVersion) >= 0;
        this.peer = peer;
        this.protocolVersion = protocolVersion;
        this.messageConsumer = messageConsumer;
    }

    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> list) throws Exception
    {
        ByteBufDataInputPlus inputPlus = new ByteBufDataInputPlus(in);
        try
        {
            if (currentFrameSize < 0)
                currentFrameSize = in.readInt();
            // TODO Figure out if we need to correct for the read size (e.g. readableBytes < currentFrameSize - 4).
            if (in.readableBytes() < currentFrameSize)
                return;
            Message.Serializer serializer = Message.createSerializer(MessagingVersion.from(protocolVersion),
                                                                     // TODO Figure out if the message-serialized timestamp should be used here.
                                                                     // See https://github.com/riptano/apollo/commit/3d4840ecc5838d0e12a5488b9c9b9c3b2e026dad#diff-6b0e578d46ab48563b955c4aa6917f2aR159
                                                                     ApproximateTime.currentTimeMillis());
            Message<Object> messageIn = serializer.deserialize(inputPlus, peer);
            currentFrameSize = -1;
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
