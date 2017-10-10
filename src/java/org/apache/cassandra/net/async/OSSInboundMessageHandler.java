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

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.cassandra.db.monitoring.ApproximateTime;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingVersion;
import org.apache.cassandra.net.OSSMessageHeader;
import org.apache.cassandra.net.OSSVerb;
import org.apache.cassandra.net.ProtocolVersion;

/**
 * Parses out individual messages from the incoming buffers. Each message, both header and payload, is incrementally
 * built up from the available input data, then passed to the {@link #messageConsumer}.
 *
 * Note: this class derives from {@link ByteToMessageDecoder} to take advantage of the {@link ByteToMessageDecoder.Cumulator}
 * behavior across {@link #decode(ChannelHandlerContext, ByteBuf, List)} invocations. That way we don't have to maintain
 * the not-fully consumed {@link ByteBuf}s.
 */
class OSSInboundMessageHandler extends ByteToMessageDecoder
{
    public static final Logger logger = LoggerFactory.getLogger(OSSInboundMessageHandler.class);

    /**
     * The default target for consuming deserialized {@link Message}.
     */
    static final Consumer<Message<?>> MESSAGING_SERVICE_CONSUMER = (message) -> MessagingService.instance().receive(message);

    private enum State
    {
        READ_FIRST_CHUNK,
        READ_IP_ADDRESS,
        READ_SECOND_CHUNK,
        READ_PARAMETERS_DATA,
        READ_PAYLOAD_SIZE,
        READ_PAYLOAD
    }

    /**
     * The byte count for magic, msg id, timestamp values.
     */
    @VisibleForTesting
    static final int FIRST_SECTION_BYTE_COUNT = 12;

    /**
     * The byte count for the verb id and the number of parameters.
     */
    private static final int SECOND_SECTION_BYTE_COUNT = 8;

    private final InetAddress peer;
    private final ProtocolVersion protocolVersion;

    /**
     * Abstracts out depending directly on {@link MessagingService#receive(Message)}; this makes tests more sane
     * as they don't require nor trigger the entire message processing circus.
     */
    private final Consumer<Message<?>> messageConsumer;

    private State state;
    private HeaderData headerData;

    OSSInboundMessageHandler(InetAddress peer, ProtocolVersion protocolVersion)
    {
        this(peer, protocolVersion, MESSAGING_SERVICE_CONSUMER);
    }

    OSSInboundMessageHandler(InetAddress peer, ProtocolVersion protocolVersion, Consumer<Message<?>> messageConsumer)
    {
        this.peer = peer;
        this.protocolVersion = protocolVersion;
        this.messageConsumer = messageConsumer;
        state = State.READ_FIRST_CHUNK;
    }

    /**
     * For each new message coming in, builds up a {@link OSSMessageHeader} instance incrementally. This method
     * attempts to deserialize as much header information as it can out of the incoming {@link ByteBuf}, and
     * maintains a trivial state machine to remember progress across invocations.
     */
    @SuppressWarnings("resource")
    public void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
    {
        ByteBufDataInputPlus inputPlus = new ByteBufDataInputPlus(in);
        try
        {
            while (true)
            {
                // an imperfect optimization around calling in.readableBytes() all the time
                int readableBytes = in.readableBytes();

                switch (state)
                {
                    case READ_FIRST_CHUNK:
                        if (readableBytes < FIRST_SECTION_BYTE_COUNT)
                            return;
                        MessagingService.validateMagic(in.readInt());

                        headerData = new HeaderData();
                        headerData.messagingVersion = MessagingVersion.from(this.protocolVersion);
                        headerData.messageId = in.readInt();
                        // make sure to read the sent timestamp, even if DatabaseDescriptor.hasCrossNodeTimeout() is not enabled
                        headerData.timestampLoBits = in.readInt();

                        state = State.READ_IP_ADDRESS;
                        readableBytes -= FIRST_SECTION_BYTE_COUNT;
                        // fall-through
                    case READ_IP_ADDRESS:
                        // unfortunately, this assumes knowledge of how CompactEndpointSerializationHelper serializes data (the first byte is the size).
                        // first, check that we can actually read the size byte, then check if we can read that number of bytes.
                        // the "+ 1" is to make sure we have the size byte in addition to the serialized IP addr count of bytes in the buffer.
                        int serializedAddrSize;
                        if (readableBytes < 1 || readableBytes < (serializedAddrSize = in.getByte(in.readerIndex()) + 1))
                            return;

                        headerData.from = CompactEndpointSerializationHelper.deserialize(inputPlus);

                        state = State.READ_SECOND_CHUNK;
                        readableBytes -= serializedAddrSize;
                        // fall-through
                    case READ_SECOND_CHUNK:
                        if (readableBytes < SECOND_SECTION_BYTE_COUNT)
                            return;

                        headerData.verb = OSSVerb.getVerbById(in.readInt());
                        headerData.parameterCount = in.readInt();
                        headerData.parameters = headerData.parameterCount == 0
                                                ? Collections.emptyMap()
                                                : new HashMap<>();

                        state = headerData.parameterCount > 0 ? State.READ_PARAMETERS_DATA : State.READ_PAYLOAD_SIZE;
                        readableBytes -= SECOND_SECTION_BYTE_COUNT;
                        // fall-through
                    case READ_PARAMETERS_DATA:
                        if (!readParameters(in, inputPlus, headerData.parameterCount, headerData.parameters))
                            return;
                        // we read an indeterminate number of bytes for the parameters, so just ask the buffer again
                        readableBytes = in.readableBytes();
                        state = State.READ_PAYLOAD_SIZE;
                        // fall-through
                    case READ_PAYLOAD_SIZE:
                        if (readableBytes < 4)
                            return;

                        headerData.payloadSize = in.readInt();

                        state = State.READ_PAYLOAD;
                        readableBytes -= 4;
                        // fall-through
                    case READ_PAYLOAD:
                        if (readableBytes < headerData.payloadSize)
                            return;

                        Message.Serializer serializer = Message.createSerializer(MessagingVersion.from(protocolVersion),
                                                                                 ApproximateTime.currentTimeMillis());
                        // TODO consider deserializing the message not on the event loop
                        // Ideally we'd like to rely on the Message.Serializer abstraction to deserialize the incoming
                        // message, preferably by reading the serialized size of the whole frame, and then waiting
                        // until all necessary bytes are available. Unfortunately there are a couple of problems with
                        // that:
                        // 1. Netty works best if either messages can be consumed in known, but variable-sized chunks
                        //    without a significant overhead, or sizing/framing info is available and can be used to
                        //    minimize decoding iterations.
                        // 2. As of MessagingVersion.OSS_40, the OSS protocol includes solely the payload (not the
                        //    message/frame) size, and that payload size can be found only after an unknown number of
                        //    header bytes (hence the fairly convoluted decode() implementation and readParameters() /
                        //    canReadNextParam() combo).
                        OSSMessageHeader header = OSSMessageHeader.from(headerData.messagingVersion,
                                                                        headerData.messageId,
                                                                        headerData.timestampLoBits,
                                                                        headerData.from,
                                                                        headerData.verb,
                                                                        headerData.payloadSize,
                                                                        headerData.parameterCount,
                                                                        headerData.parameters);
                        Message<Object> message = serializer.deserializePayload(inputPlus, header);
                        if (message != null)
                            messageConsumer.accept(message);

                        state = State.READ_FIRST_CHUNK;
                        break;
                    default:
                        throw new IllegalStateException("unknown/unhandled state: " + state);
                }
            }
        }
        catch (Exception e)
        {
            exceptionCaught(ctx, e);
        }
    }

    /**
     * @return <code>true</code> if all the parameters have been read from the {@link ByteBuf}; else, <code>false</code>.
     */
    private boolean readParameters(ByteBuf in, ByteBufDataInputPlus inputPlus, int parameterCount, Map<String, byte[]> parameters) throws IOException
    {
        // makes the assumption that map.size() is a constant time function (HashMap.size() is)
        while (parameters.size() < parameterCount)
        {
            if (!canReadNextParam(in))
                return false;

            String key = DataInputStream.readUTF(inputPlus);
            byte[] value = new byte[in.readInt()];
            in.readBytes(value);
            parameters.put(key, value);
        }

        return true;
    }

    /**
     * Determine if we can read the next parameter from the {@link ByteBuf}. This method will *always* set the {@code in}
     * readIndex back to where it was when this method was invoked.
     *
     * NOTE: this function would be sooo much simpler if we included a parameters length int in the messaging format,
     * instead of checking the remaining readable bytes for each field as we're parsing it. c'est la vie ...
     */
    @VisibleForTesting
    static boolean canReadNextParam(ByteBuf in)
    {
        in.markReaderIndex();
        // capture the readableBytes value here to avoid all the virtual function calls.
        // subtract 6 as we know we'll be reading a short and an int (for the utf and value lengths).
        final int minimumBytesRequired = 6;
        int readableBytes = in.readableBytes() - minimumBytesRequired;
        if (readableBytes < 0)
            return false;

        // this is a tad invasive, but since we know the UTF string is prefaced with a 2-byte length,
        // read that to make sure we have enough bytes to read the string itself.
        short strLen = in.readShort();
        // check if we can read that many bytes for the UTF
        if (strLen > readableBytes)
        {
            in.resetReaderIndex();
            return false;
        }
        in.skipBytes(strLen);
        readableBytes -= strLen;

        // check if we can read the value length
        if (readableBytes < 4)
        {
            in.resetReaderIndex();
            return false;
        }
        int valueLength = in.readInt();
        // check if we read that many bytes for the value
        if (valueLength > readableBytes)
        {
            in.resetReaderIndex();
            return false;
        }

        in.resetReaderIndex();
        return true;
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

    // should ony be used for testing!!!
    @VisibleForTesting
    HeaderData getMessageHeader()
    {
        return headerData;
    }

    @VisibleForTesting
    static class HeaderData
    {
        MessagingVersion messagingVersion;
        int messageId;
        int timestampLoBits;
        InetAddress from;
        OSSVerb verb;
        int payloadSize;
        int parameterCount;
        Map<String, byte[]> parameters;
    }
}
