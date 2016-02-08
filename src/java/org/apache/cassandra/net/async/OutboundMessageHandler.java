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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.UnsupportedMessageTypeException;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingVersion;
import org.apache.cassandra.net.ProtocolVersion;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.NanoTimeToCurrentTimeMillis;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.config.Config.PROPERTY_PREFIX;

/**
 * A Netty {@link ChannelHandler} for serializing outbound messages.
 * <p>
 * On top of transforming a {@link QueuedMessage} into bytes, this handler also feeds back progress to the linked
 * {@link ChannelWriter} so that the latter can take decision on when data should be flushed (with and without coalescing).
 * See the javadoc on {@link ChannelWriter} for more details about the callbacks as well as message timeouts.
 *<p>
 * Note: this class derives from {@link ChannelDuplexHandler} so we can intercept calls to
 * {@link #userEventTriggered(ChannelHandlerContext, Object)} and {@link #channelWritabilityChanged(ChannelHandlerContext)}.
 */
class MessageOutHandler extends ChannelDuplexHandler
{
    private static final Logger logger = LoggerFactory.getLogger(MessageOutHandler.class);
    private static final NoSpamLogger errorLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.SECONDS);

    /**
     * The default size threshold for deciding when to auto-flush the channel.
     */
    private static final int DEFAULT_AUTO_FLUSH_THRESHOLD = 1 << 16;

    // reatining the pre 4.0 property name for backward compatibility.
    private static final String AUTO_FLUSH_PROPERTY = PROPERTY_PREFIX + "otc_buffer_size";
    static final int AUTO_FLUSH_THRESHOLD = Integer.getInteger(AUTO_FLUSH_PROPERTY, DEFAULT_AUTO_FLUSH_THRESHOLD);

    /**
     * The amount of prefix data, in bytes, before the serialized message.
     */
    private static final int MESSAGE_PREFIX_SIZE = 12;

    private final OutboundConnectionIdentifier connectionId;

    /**
     * The version of the messaging protocol we're communicating at.
     */
    private final ProtocolVersion targetProtocolVersion;

    /**
     * The minumum size at which we'll automatically flush the channel.
     */
    private final int flushSizeThreshold;

    private final ChannelWriter channelWriter;

    private final Supplier<QueuedMessage> backlogSupplier;

    MessageOutHandler(OutboundConnectionIdentifier connectionId, ProtocolVersion targetProtocolVersion, ChannelWriter channelWriter, Supplier<QueuedMessage> backlogSupplier)
    {
        this (connectionId, targetProtocolVersion, channelWriter, backlogSupplier, AUTO_FLUSH_THRESHOLD);
    }

    MessageOutHandler(OutboundConnectionIdentifier connectionId, ProtocolVersion targetProtocolVersion, ChannelWriter channelWriter, Supplier<QueuedMessage> backlogSupplier, int flushThreshold)
    {
        this.connectionId = connectionId;
        this.targetProtocolVersion = targetProtocolVersion;
        this.channelWriter = channelWriter;
        this.flushSizeThreshold = flushThreshold;
        this.backlogSupplier = backlogSupplier;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object o, ChannelPromise promise)
    {
        // this is a temporary fix until https://github.com/netty/netty/pull/6867 is released (probably netty 4.1.13).
        // TL;DR a closed channel can still process messages in the pipeline that were queued before the close.
        // the channel handlers are removed from the channel potentially saync from the close operation.
        if (!ctx.channel().isOpen())
        {
            logger.debug("attempting to process a message in the pipeline, but the channel is closed", ctx.channel().id());
            return;
        }

        ByteBuf out = null;
        try
        {
            if (!isMessageValid(o, promise))
                return;

            QueuedMessage msg = (QueuedMessage) o;
            Message.Serializer serializer = Message.createSerializer(MessagingVersion.from(targetProtocolVersion),
                                                                     System.currentTimeMillis());
            long serializedSize = serializer.serializedSize(msg.message);
            assert serializedSize <= Integer.MAX_VALUE : "Invalid message, too large: " + serializedSize;
            int messageSize = (int)serializedSize;

            Tracing.instance.onMessageSend(msg.message, messageSize);

            DataOutputPlus outStream = new ByteBufDataOutputPlus(out);
            serializer.writeSerializedSize(messageSize, outStream);
            serializer.serialize(msg.message, outStream);
            ctx.write(out, promise);

            // check to see if we should flush based on buffered size
            ChannelOutboundBuffer outboundBuffer = ctx.channel().unsafe().outboundBuffer();
            if (outboundBuffer != null && outboundBuffer.totalPendingWriteBytes() >= flushSizeThreshold)
                ctx.flush();
        }
        catch(Exception e)
        {
            if (out != null && out.refCnt() > 0)
                out.release(out.refCnt());
            exceptionCaught(ctx, e);
            promise.tryFailure(e);
        }
        finally
        {
            // Make sure we signal the outChanel even in case of errors.
            channelWriter.onMessageProcessed(ctx);
        }
    }

    /**
     * Test to see if the message passed in is a {@link QueuedMessage} and if it has timed out or not. If the checks fail,
     * this method has the side effect of modifying the {@link ChannelPromise}.
     */
    boolean isMessageValid(Object o, ChannelPromise promise)
    {
        // optimize for the common case
        if (o instanceof QueuedMessage)
        {
            if (!((QueuedMessage)o).isTimedOut())
            {
                return true;
            }
            else
            {
                promise.tryFailure(ExpiredException.INSTANCE);
            }
        }
        else
        {
            promise.tryFailure(new UnsupportedMessageTypeException(connectionId +
                                                                   " msg must be an instance of " + QueuedMessage.class.getSimpleName()));
        }
        return false;
    }

    @Override
    public void flush(ChannelHandlerContext ctx)
    {
        channelWriter.onTriggeredFlush(ctx);
    }


    /**
     * {@inheritDoc}
     *
     * When the channel becomes writable (assuming it was previously unwritable), try to eat through any backlogged messages
     * {@link #backlogSupplier}. As we're on the event loop when this is invoked, no one else can fill up the netty
     * {@link ChannelOutboundBuffer}, so we should be able to make decent progress chewing through the backlog
     * (assuming not large messages). Any messages messages written from {@link OutboundMessagingConnection} threads won't
     * be processed immediately; they'll be queued up as tasks, and once this function return, those messages can begin
     * to be consumed.
     * <p>
     * Note: this is invoked on the netty event loop.
     */
    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx)
    {
        if (!ctx.channel().isWritable())
            return;

        // guarantee at least a minimal amount of progress (one messge from the backlog) by using a do-while loop.
        do
        {
            QueuedMessage msg = backlogSupplier.get();
            if (msg == null || !channelWriter.write(msg, false))
                break;
        } while (ctx.channel().isWritable());
    }

    /**
     * {@inheritDoc}
     *
     * If we get an {@link IdleStateEvent} for the write path, we want to close the channel as we can't make progress.
     * That assumes, of course, that there's any outstanding bytes in the channel to write. We don't necesarrily care
     * about idleness (for example, gossip channels will be idle most of the time), but instead our concern is
     * the ability to make progress when there's work to be done.
     * <p>
     * Note: this is invoked on the netty event loop.
     */
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt)
    {
        if (evt instanceof IdleStateEvent && ((IdleStateEvent)evt).state() == IdleState.WRITER_IDLE)
        {
            ChannelOutboundBuffer cob = ctx.channel().unsafe().outboundBuffer();
            if (cob != null && cob.totalPendingWriteBytes() > 0)
            {
                ctx.channel().attr(ChannelWriter.PURGE_MESSAGES_CHANNEL_ATTR)
                   .compareAndSet(Boolean.FALSE, Boolean.TRUE);
                ctx.close();
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
        if (cause instanceof IOException)
            logger.trace("{} io error", connectionId, cause);
        else
            logger.warn("{} error", connectionId, cause);

        ctx.close();
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise)
    {
        ctx.flush();
        ctx.close(promise);
    }
}
