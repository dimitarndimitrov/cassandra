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
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import com.google.common.base.Charsets;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.monitoring.ApproximateTime;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.EmptyPayload;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageParameters;
import org.apache.cassandra.net.MessagingVersion;
import org.apache.cassandra.net.ProtocolVersion;
import org.apache.cassandra.net.Verbs;

public class InboundMessageHandlerTest
{
    private static final InetSocketAddress addr = new InetSocketAddress("127.0.0.1", 0);
    private static final ProtocolVersion CURRENT_VERSION = MessagingVersion.DSE_60.protocolVersion();

    private ByteBuf buf;

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @After
    public void tearDown()
    {
        if (buf != null && buf.refCnt() > 0)
            buf.release();
    }

    @Test
    public void decode_HappyPath_NoParameters() throws Exception
    {
        Message<?> result = decode_HappyPath(Collections.emptyMap());
        Assert.assertTrue(result.parameters().isEmpty());
    }

    @Test
    public void decode_HappyPath_WithParameters() throws Exception
    {
        Map<String, byte[]> parameters = new HashMap<>();
        parameters.put("p1", "val1".getBytes(Charsets.UTF_8));
        parameters.put("p2", "val2".getBytes(Charsets.UTF_8));
        Message<?> result = decode_HappyPath(parameters);
        Assert.assertEquals(2, result.parameters().size());
    }

    private Message<?> decode_HappyPath(Map<String, byte[]> parameters) throws Exception
    {
        Message outboundMessage = Verbs.GOSSIP.SHUTDOWN.newRequest(addr.getAddress(), EmptyPayload.instance);
        int msgId = outboundMessage.id();
        outboundMessage = outboundMessage.addParameters(MessageParameters.from(parameters));
        serialize(outboundMessage);

        MessageWrapper wrapper = new MessageWrapper();
        InboundMessageHandler handler = new InboundMessageHandler(addr.getAddress(), CURRENT_VERSION, wrapper.consumer);
        List<Object> out = new ArrayList<>();
        handler.decode(null, buf, out);

        Assert.assertNotNull(wrapper.message);
        Assert.assertEquals(msgId, wrapper.message.id());
        Assert.assertEquals(outboundMessage.from(), wrapper.message.from());
        Assert.assertEquals(outboundMessage.verb(), wrapper.message.verb());
        Assert.assertTrue(out.isEmpty());

        return wrapper.message;
    }

    @Test
    public void decode_With1ByteReceived() throws Exception
    {
        Message outboundMessage = Verbs.GOSSIP.SHUTDOWN.newRequest(addr.getAddress(), EmptyPayload.instance);
        int msgId = outboundMessage.id();
        serialize(outboundMessage);

        MessageWrapper wrapper = new MessageWrapper();
        InboundMessageHandler handler = new InboundMessageHandler(addr.getAddress(), CURRENT_VERSION, wrapper.consumer);
        List<Object> out = new ArrayList<>();

        int serializedSize = buf.writerIndex();
        Assert.assertTrue(serializedSize > 5);
        // Nothing should break here if we cannot even read the message size...
        buf.writerIndex(1);
        handler.decode(null, buf, out);
        Assert.assertNull(wrapper.message);
        // ...or if we can read just the message size...
        buf.writerIndex(4);
        handler.decode(null, buf, out);
        Assert.assertNull(wrapper.message);
        // ...or if we can read after the message size, but cannot read the whole message.
        buf.writerIndex(5);
        handler.decode(null, buf, out);
        Assert.assertNull(wrapper.message);

        buf.writerIndex(serializedSize);
        handler.decode(null, buf, out);
        Assert.assertNotNull(wrapper.message);
        Assert.assertEquals(msgId, wrapper.message.id());
        Assert.assertEquals(outboundMessage.from(), wrapper.message.from());
        Assert.assertEquals(outboundMessage.verb(), wrapper.message.verb());
        Assert.assertTrue(out.isEmpty());
    }

    private void serialize(Message<?> outboundMessage) throws IOException
    {
        buf = Unpooled.buffer(1024, 1024); // 1k should be enough for everybody!
        Message.Serializer serializer = Message.createSerializer(MessagingVersion.from(CURRENT_VERSION),
                                                                 ApproximateTime.currentTimeMillis());
        long serializedSize = serializer.serializedSize(outboundMessage);
        Assert.assertTrue(serializedSize <= Integer.MAX_VALUE);
        DataOutputPlus outputPlus = new ByteBufDataOutputPlus(buf);
        serializer.writeSerializedSize((int) serializedSize, outputPlus);
        serializer.serialize(outboundMessage, outputPlus);
    }

    @Test
    public void exceptionHandled()
    {
        InboundMessageHandler handler = new InboundMessageHandler(addr.getAddress(), CURRENT_VERSION, null);
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        Assert.assertTrue(channel.isOpen());
        handler.exceptionCaught(channel.pipeline().firstContext(), new EOFException());
        Assert.assertFalse(channel.isOpen());
    }

    private static class MessageWrapper
    {
        Message<?> message;

        final Consumer<Message<?>> consumer = (message) ->
        {
            this.message = message;
        };
    }
}
