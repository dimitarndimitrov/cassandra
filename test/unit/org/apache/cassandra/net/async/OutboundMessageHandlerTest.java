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
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.UnsupportedMessageTypeException;
import io.netty.handler.timeout.IdleStateEvent;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.EmptyPayload;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ProtocolVersion;
import org.apache.cassandra.net.Response;
import org.apache.cassandra.net.Verbs;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;

public class OutboundMessageHandlerTest
{
    // TODO Should all tests be duplicated - one instance going through the legacy (OSS) serialization path,
    // and another going through the new serialization path, that came with the MessagingService refactoring?
    private static final ProtocolVersion CURRENT_VERSION = MessagingService.current_version.protocolVersion();

    private static final InetSocketAddress FROM = new InetSocketAddress("127.0.0.1", 0);
    private static final InetSocketAddress TO = new InetSocketAddress("127.0.0.2", 0);

    private static final Message<EmptyPayload> newDummyResponse()
    {
        return Response.testResponse(FROM.getAddress(), TO.getAddress(), Verbs.WRITES.WRITE, EmptyPayload.instance);
    };

    private ChannelWriter channelWriter;
    private EmbeddedChannel channel;
    private OutboundMessageHandler handler;

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.createAllDirectories();
    }

    @Before
    public void setup()
    {
        setup(OutboundMessageHandler.AUTO_FLUSH_THRESHOLD);
    }

    private void setup(int flushThreshold)
    {
        OutboundConnectionIdentifier connectionId = OutboundConnectionIdentifier.small(FROM, TO);
        OutboundMessagingConnection omc = new NonSendingOutboundMessagingConnection(connectionId, null, Optional.empty());
        channel = new EmbeddedChannel();
        channelWriter = ChannelWriter.create(channel, omc::handleMessageResult, Optional.empty());
        handler = new OutboundMessageHandler(connectionId, CURRENT_VERSION, channelWriter, () -> null, flushThreshold);
        channel.pipeline().addLast(handler);
    }

    @Test
    public void write_NoFlush() throws ExecutionException, InterruptedException, TimeoutException, UnknownHostException
    {
        Message outboundMessage = Verbs.GOSSIP.ECHO.newRequest(FROM.getAddress(), EmptyPayload.instance);
        ChannelFuture future = channel.write(new QueuedMessage(outboundMessage, 42));
        Assert.assertTrue(!future.isDone());
        Assert.assertFalse(channel.releaseOutbound());
    }

    @Test
    public void write_WithFlush() throws ExecutionException, InterruptedException, TimeoutException, UnknownHostException
    {
        setup(1);
        Message outboundMessage = Verbs.GOSSIP.ECHO.newRequest(FROM.getAddress(), EmptyPayload.instance);
        ChannelFuture future = channel.write(new QueuedMessage(outboundMessage, 42));
        Assert.assertTrue(future.isSuccess());
        Assert.assertTrue(channel.releaseOutbound());
    }

    @Test
    public void serializeMessage() throws IOException
    {
        channelWriter.pendingMessageCount.set(1);
        Message<?> dummyResponse = newDummyResponse();
        QueuedMessage msg = new QueuedMessage(dummyResponse, dummyResponse.id());
        ChannelFuture future = channel.writeAndFlush(msg);

        Assert.assertTrue(future.isSuccess());
        Assert.assertTrue(1 <= channel.outboundMessages().size());
        Assert.assertTrue(channel.releaseOutbound());
    }

    @Test
    public void wrongMessageType()
    {
        ChannelPromise promise = new DefaultChannelPromise(channel);
        Assert.assertFalse(handler.isMessageValid("this is the wrong message type", promise));

        Assert.assertFalse(promise.isSuccess());
        Assert.assertNotNull(promise.cause());
        Assert.assertSame(UnsupportedMessageTypeException.class, promise.cause().getClass());
    }

    @Test
    public void unexpiredMessage()
    {
        long timeoutMillis = 5000l;
        Message<?> expiredMessage = Response.testResponse(FROM.getAddress(),
                                                          TO.getAddress(),
                                                          Verbs.WRITES.WRITE,
                                                          EmptyPayload.instance,
                                                          0l,
                                                          // Message<P> creation timestamp IS NOT important for this test
                                                          0l,
                                                          // Message<P> timeout duration IS important for this test
                                                          timeoutMillis);
        QueuedMessage msg = new QueuedMessage(expiredMessage, expiredMessage.id(), System.nanoTime(), true);
        ChannelPromise promise = new DefaultChannelPromise(channel);
        Assert.assertTrue(handler.isMessageValid(msg, promise));

        Assert.assertNull(promise.cause());
    }

    @Test
    public void expiredMessage()
    {
        long timeoutMillis = 5000l;
        Message<?> expiredMessage = Response.testResponse(FROM.getAddress(),
                                                          TO.getAddress(),
                                                          Verbs.WRITES.WRITE,
                                                          EmptyPayload.instance,
                                                          0l,
                                                          // Message<P> creation timestamp IS NOT important for this test
                                                          0l,
                                                          // Message<P> timeout duration IS important for this test
                                                          timeoutMillis);
        long timedOutTimestamp = System.currentTimeMillis() - TimeUnit.MILLISECONDS.toNanos(2 * timeoutMillis);
        QueuedMessage msg = new QueuedMessage(expiredMessage, expiredMessage.id(), timedOutTimestamp, true);
        ChannelPromise promise = new DefaultChannelPromise(channel);
        Assert.assertFalse(handler.isMessageValid(msg, promise));

        Assert.assertFalse(promise.isSuccess());
        Assert.assertNotNull(promise.cause());
        Assert.assertSame(ExpiredException.class, promise.cause().getClass());
        Assert.assertTrue(channel.outboundMessages().isEmpty());
    }

    // TODO Enforce payload size checking. With CASSANDRA-8457, payload size is enforced (and bounded) more rigorously,
    // but in the current version of the merged code, the lax treatment that existed before that (in both OSS and
    // Apollo) is being used.
//    @Test
//    public void write_MessageTooLarge()
//    {
//        write_BadMessageSize(Integer.MAX_VALUE);
//    }
//
//    @Test
//    public void write_MessageSizeIsBananas()
//    {
//        write_BadMessageSize(Integer.MIN_VALUE + 10000);
//    }
//
//    private void write_BadMessageSize(int size)
//    {
//        Message<?> badSizeResponse = Response.testResponse(FROM.getAddress(), TO.getAddress(), Verbs.WRITES.WRITE, EmptyPayload.instance, size);
//        ChannelFuture future = channel.write(new QueuedMessage(badSizeResponse, badSizeResponse.id()));
//        Throwable t = future.cause();
//        Assert.assertNotNull(t);
//        Assert.assertSame(IllegalStateException.class, t.getClass());
//        Assert.assertTrue(channel.isOpen());
//        Assert.assertFalse(channel.releaseOutbound());
//    }

    @Test
    @BMRule(name = "Force exception during write",
            targetClass = "org.apache.cassandra.net.MessageSerializer",
            targetMethod = "<init>(org.apache.cassandra.net.MessagingVersion, long)",
            action = "throw new RuntimeException(\"this exception is part of the test - DON'T PANIC\")")
    public void writeForceExceptionPath()
    {
        Message<?> dummyResponse = newDummyResponse();
        ChannelFuture future = channel.write(new QueuedMessage(dummyResponse, dummyResponse.id()));
        Throwable t = future.cause();
        Assert.assertNotNull(t);
        Assert.assertFalse(channel.isOpen());
        Assert.assertFalse(channel.releaseOutbound());
    }

    // TODO Figure out how (and if) tracing tests should be implemented.

    @Test
    public void userEventTriggered_RandomObject()
    {
        Assert.assertTrue(channel.isOpen());
        ChannelUserEventSender sender = new ChannelUserEventSender();
        channel.pipeline().addFirst(sender);
        sender.sendEvent("ThisIsAFakeEvent");
        Assert.assertTrue(channel.isOpen());
    }

    @Test
    public void userEventTriggered_Idle_NoPendingBytes()
    {
        Assert.assertTrue(channel.isOpen());
        ChannelUserEventSender sender = new ChannelUserEventSender();
        channel.pipeline().addFirst(sender);
        sender.sendEvent(IdleStateEvent.WRITER_IDLE_STATE_EVENT);
        Assert.assertTrue(channel.isOpen());
    }

    @Test
    public void userEventTriggered_Idle_WithPendingBytes()
    {
        Assert.assertTrue(channel.isOpen());
        ChannelUserEventSender sender = new ChannelUserEventSender();
        channel.pipeline().addFirst(sender);

        Message<?> dummyResponse = newDummyResponse();
        channel.writeOutbound(new QueuedMessage(dummyResponse, dummyResponse.id()));
        sender.sendEvent(IdleStateEvent.WRITER_IDLE_STATE_EVENT);
        Assert.assertFalse(channel.isOpen());
    }

    private static class ChannelUserEventSender extends ChannelOutboundHandlerAdapter
    {
        private ChannelHandlerContext ctx;

        @Override
        public void handlerAdded(final ChannelHandlerContext ctx) throws Exception
        {
            this.ctx = ctx;
        }

        private void sendEvent(Object event)
        {
            ctx.fireUserEventTriggered(event);
        }
    }
}
