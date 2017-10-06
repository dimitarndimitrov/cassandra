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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.AllowAllInternodeAuthenticator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.GossipDigestSyn;
import org.apache.cassandra.net.BackPressureState;
import org.apache.cassandra.net.EmptyPayload;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Response;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.net.async.OutboundConnectionIdentifier.ConnectionType;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;

public class OutboundMessagingPoolTest
{
    private static final InetSocketAddress LOCAL_ADDR = new InetSocketAddress("127.0.0.1", 9476);
    private static final InetSocketAddress REMOTE_ADDR = new InetSocketAddress("127.0.0.2", 9476);
    private static final InetSocketAddress RECONNECT_ADDR = new InetSocketAddress("127.0.0.3", 9476);
    private static final List<ConnectionType> INTERNODE_MESSAGING_CONN_TYPES = new ArrayList<ConnectionType>()
            {{ add(ConnectionType.GOSSIP); add(ConnectionType.LARGE_MESSAGE); add(ConnectionType.SMALL_MESSAGE); }};

    private OutboundMessagingPool pool;

    private static final Message<EmptyPayload> newDummyResponse()
    {
        return Response.testResponse(LOCAL_ADDR.getAddress(), REMOTE_ADDR.getAddress(), Verbs.WRITES.WRITE, EmptyPayload.instance);
    }

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setup()
    {
        BackPressureState backPressureState = DatabaseDescriptor.getBackPressureStrategy().newState(REMOTE_ADDR.getAddress());
        pool = new OutboundMessagingPool(REMOTE_ADDR, LOCAL_ADDR, null, backPressureState, new AllowAllInternodeAuthenticator());
    }

    @After
    public void tearDown()
    {
        if (pool != null)
            pool.close(false);
    }

    @Test
    public void getConnection_Gossip()
    {
        GossipDigestSyn syn = new GossipDigestSyn("cluster", "partitioner", new ArrayList<>(0));
        Message<GossipDigestSyn> gossipMessage = Verbs.GOSSIP.SYN.newRequest(LOCAL_ADDR.getAddress(), syn);
        Assert.assertEquals(ConnectionType.GOSSIP, pool.getConnection(gossipMessage).getConnectionId().type());
    }

    @Test
    public void getConnection_SmallMessage()
    {
        Message<?> dummyResponse = newDummyResponse();
        Assert.assertEquals(ConnectionType.SMALL_MESSAGE, pool.getConnection(dummyResponse).getConnectionId().type());
    }

    @Test
    @BMRules(rules = { @BMRule(name = "Tweak serialized size",
                               targetClass = "org.apache.cassandra.net.MessageSerializer",
                               targetMethod = "getSerializedSize",
                               action = "return $largeMessageSize;") } )
    public void getConnection_LargeMessage()
    {

        long largeMessageSize = OutboundMessagingPool.LARGE_MESSAGE_THRESHOLD + 1;

        Message<?> dummyResponse = newDummyResponse();
        Assert.assertEquals(ConnectionType.LARGE_MESSAGE, pool.getConnection(dummyResponse).getConnectionId().type());
    }

    @Test
    public void close()
    {
        for (ConnectionType type : INTERNODE_MESSAGING_CONN_TYPES)
            Assert.assertNotSame(OutboundMessagingConnection.State.CLOSED, pool.getConnection(type).getState());
        pool.close(false);
        for (ConnectionType type : INTERNODE_MESSAGING_CONN_TYPES)
            Assert.assertEquals(OutboundMessagingConnection.State.CLOSED, pool.getConnection(type).getState());
    }

    @Test
    public void reconnectWithNewIp()
    {
        for (ConnectionType type : INTERNODE_MESSAGING_CONN_TYPES)
        {
            Assert.assertEquals(REMOTE_ADDR, pool.getPreferredRemoteAddr());
            Assert.assertEquals(REMOTE_ADDR, pool.getConnection(type).getConnectionId().connectionAddress());
        }

        pool.reconnectWithNewIp(RECONNECT_ADDR);

        for (ConnectionType type : INTERNODE_MESSAGING_CONN_TYPES)
        {
            Assert.assertEquals(RECONNECT_ADDR, pool.getPreferredRemoteAddr());
            Assert.assertEquals(RECONNECT_ADDR, pool.getConnection(type).getConnectionId().connectionAddress());
        }
    }

    @Test
    public void timeoutCounter()
    {
        long originalValue = pool.getTimeouts();
        pool.incrementTimeout();
        Assert.assertEquals(originalValue + 1, pool.getTimeouts());
    }
}
