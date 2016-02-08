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
import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.monitoring.ApproximateTime;
import org.apache.cassandra.net.EmptyPayload;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageParameters;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingVersion;
import org.apache.cassandra.net.ProtocolVersion;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.net.async.InboundOSSMessageHandler.MessageHeader;

public class InboundOSSMessageHandlerTest
{
    private static final InetSocketAddress addr = new InetSocketAddress("127.0.0.1", 0);
    private static final ProtocolVersion CURRENT_VERSION = MessagingService.current_version.protocolVersion();

    private static final int MSG_ID = 42;

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
    public void decode_BadMagic() throws Exception
    {
        int len = InboundOSSMessageHandler.FIRST_SECTION_BYTE_COUNT;
        buf = Unpooled.buffer(len, len);
        buf.writeInt(-1);
        buf.writerIndex(len);

        InboundOSSMessageHandler handler = new InboundOSSMessageHandler(addr.getAddress(), CURRENT_VERSION, null);
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        Assert.assertTrue(channel.isOpen());
        channel.writeInbound(buf);
        Assert.assertFalse(channel.isOpen());
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
        Message outboundMessage = Verbs.GOSSIP.ECHO.newRequest(addr.getAddress(), EmptyPayload.instance);
        outboundMessage = outboundMessage.addParameters(MessageParameters.from(parameters));
        serialize(outboundMessage);

        MessageWrapper wrapper = new MessageWrapper();
        InboundOSSMessageHandler handler = new InboundOSSMessageHandler(addr.getAddress(), CURRENT_VERSION, wrapper.consumer);
        List<Object> out = new ArrayList<>();
        handler.decode(null, buf, out);

        Assert.assertNotNull(wrapper.message);
        Assert.assertEquals(MSG_ID, wrapper.message.id());
        Assert.assertEquals(outboundMessage.from(), wrapper.message.from());
        Assert.assertEquals(outboundMessage.verb(), wrapper.message.verb());
        Assert.assertTrue(out.isEmpty());

        return wrapper.message;
    }

    private void serialize(Message<?> outMsg) throws IOException
    {
        buf = Unpooled.buffer(1024, 1024); // 1k should be enough for everybody!
        Message.Serializer serializer = Message.createSerializer(MessagingVersion.from(CURRENT_VERSION),
                                                                 ApproximateTime.currentTimeMillis());
        serializer.serialize(outMsg, new ByteBufDataOutputPlus(buf));
    }

    @Test
    public void decode_WithHalfReceivedParameters() throws Exception
    {
        Message outboundMessage = Verbs.GOSSIP.ECHO.newRequest(addr.getAddress(), EmptyPayload.instance);
        outboundMessage = outboundMessage.addParameters(MessageParameters.from(ImmutableMap.of("p3", "val1".getBytes(Charsets.UTF_8))));
        serialize(outboundMessage);

        // move the write index pointer back a few bytes to simulate like the full bytes are not present.
        // yeah, it's lame, but it tests the basics of what is happening during the deserialiization
        int originalWriterIndex = buf.writerIndex();
        buf.writerIndex(originalWriterIndex - 6);

        MessageWrapper wrapper = new MessageWrapper();
        InboundOSSMessageHandler handler = new InboundOSSMessageHandler(addr.getAddress(), CURRENT_VERSION, wrapper.consumer);
        List<Object> out = new ArrayList<>();
        handler.decode(null, buf, out);

        Assert.assertNull(wrapper.message);

        MessageHeader header = handler.getMessageHeader();
        Assert.assertEquals(MSG_ID, wrapper.);
        Assert.assertEquals(outboundMessage.verb(), );
        Assert.assertEquals(outboundMessage.from(), header.from);
        Assert.assertTrue(out.isEmpty());

        // now, set the writer index back to the original value to pretend that we actually got more bytes in
        buf.writerIndex(originalWriterIndex);
        handler.decode(null, buf, out);
        Assert.assertNotNull(wrapper.message);
        Assert.assertTrue(out.isEmpty());
    }

    @Test
    public void canReadNextParam_HappyPath() throws IOException
    {
        buildParamBuf(13);
        Assert.assertTrue(InboundOSSMessageHandler.canReadNextParam(buf));
    }

    @Test
    public void canReadNextParam_OnlyFirstByte() throws IOException
    {
        buildParamBuf(13);
        buf.writerIndex(1);
        Assert.assertFalse(InboundOSSMessageHandler.canReadNextParam(buf));
    }

    @Test
    public void canReadNextParam_PartialUTF() throws IOException
    {
        buildParamBuf(13);
        buf.writerIndex(5);
        Assert.assertFalse(InboundOSSMessageHandler.canReadNextParam(buf));
    }

    @Test
    public void canReadNextParam_TruncatedValueLength() throws IOException
    {
        buildParamBuf(13);
        buf.writerIndex(buf.writerIndex() - 13 - 2);
        Assert.assertFalse(InboundOSSMessageHandler.canReadNextParam(buf));
    }

    @Test
    public void canReadNextParam_MissingLastBytes() throws IOException
    {
        buildParamBuf(13);
        buf.writerIndex(buf.writerIndex() - 2);
        Assert.assertFalse(InboundOSSMessageHandler.canReadNextParam(buf));
    }

    private void buildParamBuf(int valueLength) throws IOException
    {
        buf = Unpooled.buffer(1024, 1024); // 1k should be enough for everybody!
        ByteBufDataOutputPlus output = new ByteBufDataOutputPlus(buf);
        output.writeUTF("name");
        byte[] array = new byte[valueLength];
        output.writeInt(array.length);
        output.write(array);
    }

    @Test
    public void exceptionHandled()
    {
        InboundOSSMessageHandler handler = new InboundOSSMessageHandler(addr.getAddress(), CURRENT_VERSION, null);
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
