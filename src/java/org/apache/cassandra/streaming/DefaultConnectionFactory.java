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
package org.apache.cassandra.streaming;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;

import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.utils.FBUtilities;
public class DefaultConnectionFactory implements StreamConnectionFactory
{
    private static final Logger logger = LoggerFactory.getLogger(DefaultConnectionFactory.class);

    private static final int MAX_CONNECT_ATTEMPTS = 3;

    /**
     * Connect to peer and start exchanging message.
     * When connect attempt fails, this retries for maximum of MAX_CONNECT_ATTEMPTS times.
     *
     * @param peer the peer to connect to.
     * @return the created socket.
     *
     * @throws IOException when connection failed.
     */
    public Socket createConnection(InetAddress peer) throws IOException
    {
        int attempts = 0;
        while (true)
        {
            try
            {
                Socket socket = newSocket(peer);
                socket.setKeepAlive(true);
                return socket;
            }
            catch (IOException e)
            {
                if (++attempts >= MAX_CONNECT_ATTEMPTS)
                    throw e;

                long waitms = DatabaseDescriptor.getRpcTimeout() * (long)Math.pow(2, attempts);
                logger.warn("Failed attempt {} to connect to {}. Retrying in {} ms. ({})", attempts, peer, waitms, e.getMessage());
                try
                {
                    Thread.sleep(waitms);
                }
                catch (InterruptedException wtf)
                {
                    throw new IOException("interrupted", wtf);
                }
            }
        }
    }

    // TODO this is deliberately copied from (the now former) OutboundTcpConnectionPool, for CASSANDRA-8457.
    // to be replaced in CASSANDRA-12229 (make streaming use 8457)
    public static Socket newSocket(InetAddress endpoint) throws IOException
    {
        // zero means 'bind on any available port.'
        if (MessagingService.isEncryptedConnection(endpoint))
        {
            return SSLFactory.getSocket(DatabaseDescriptor.getServerEncryptionOptions(), endpoint, DatabaseDescriptor.getSSLStoragePort());
        }
        else
        {
            SocketChannel channel = SocketChannel.open();
            channel.connect(new InetSocketAddress(endpoint, DatabaseDescriptor.getStoragePort()));
            return channel.socket();
        }
    }
}
