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

import java.io.IOError;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import org.apache.cassandra.auth.IInternodeAuthenticator;
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.concurrent.TracingAwareExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.monitoring.ApproximateTime;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InternalRequestExecutionException;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.ILatencySubscriber;
import org.apache.cassandra.metrics.ConnectionMetrics;
import org.apache.cassandra.metrics.DroppedMessageMetrics;
import org.apache.cassandra.metrics.MessagingMetrics;
import org.apache.cassandra.net.async.NettyFactory;
import org.apache.cassandra.net.async.NettyFactory.InboundInitializer;
import org.apache.cassandra.net.async.OutboundMessagingPool;
import org.apache.cassandra.net.interceptors.Interceptor;
import org.apache.cassandra.net.interceptors.Interceptors;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ExpiringMap;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

public final class MessagingService implements MessagingServiceMBean
{
    private static final Logger logger = LoggerFactory.getLogger(MessagingService.class);

    public static final String MBEAN_NAME = "org.apache.cassandra.net:type=MessagingService";

    public static final MessagingVersion current_version = MessagingVersion.DSE_60;

    /**
     * We preface every connection handshake with this number so the recipient can validate
     * the sender is sane. For legacy OSS protocol, this is done for every message.
     */
    public static final int PROTOCOL_MAGIC = 0xCA552DFA;

    private static final AtomicInteger idGen = new AtomicInteger(0);

    /**
     * Message interceptors: either populated in the ctor through {@link Interceptors#load(String)}, or programmatically by
     * calling {@link #addInterceptor}/{@link #clearInterceptors()} for tests.
     */
    private final Interceptors messageInterceptors = new Interceptors();

    public final MessagingMetrics metrics = new MessagingMetrics();

    /* This records all the results mapped by message Id */
    private final ExpiringMap<Integer, CallbackInfo<?>> callbacks;

    @VisibleForTesting
    final ConcurrentMap<InetAddress, OutboundMessagingPool> channelManagers = new NonBlockingHashMap<>();
    final List<ServerChannel> serverChannels = Lists.newArrayList();

    private final SimpleCondition listenGate;

    private final DroppedMessages droppedMessages = new DroppedMessages();

    private final List<ILatencySubscriber> subscribers = new ArrayList<>();

    // protocol versions of the other nodes in the cluster
    private final ConcurrentMap<InetAddress, MessagingVersion> versions = new NonBlockingHashMap<>();

    // back-pressure implementation
    private final BackPressureStrategy backPressure = DatabaseDescriptor.getBackPressureStrategy();

    private static class MSHandle
    {
        public static final MessagingService instance = new MessagingService(false);
    }

    public static MessagingService instance()
    {
        return MSHandle.instance;
    }

    private static class MSTestHandle
    {
        public static final MessagingService instance = new MessagingService(true);
    }

    static MessagingService test()
    {
        return MSTestHandle.instance;
    }

    private MessagingService(boolean testOnly)
    {
        listenGate = new SimpleCondition();

        if (!testOnly)
            droppedMessages.scheduleLogging();

        Consumer<Pair<Integer, ExpiringMap.CacheableObject<CallbackInfo<?>>>> timeoutReporter = pair ->
        {
            CallbackInfo expiredCallbackInfo = pair.right.get();
            MessageCallback<?> callback = expiredCallbackInfo.callback;
            InetAddress target = expiredCallbackInfo.target;

            addLatency(expiredCallbackInfo.verb, target, pair.right.timeoutMillis());

                ConnectionMetrics.totalTimeouts.mark();
                markTimeout(expiredCallbackInfo.target);

            updateBackPressureOnReceive(target, expiredCallbackInfo.verb, true).join();
            StageManager.getStage(Stage.INTERNAL_RESPONSE).submit(() -> callback.onTimeout(target));
        };
        callbacks = new ExpiringMap<>(DatabaseDescriptor.getMinRpcTimeout(), timeoutReporter);

        if (!testOnly)
        {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            try
            {
                mbs.registerMBean(this, new ObjectName(MBEAN_NAME));
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        messageInterceptors.load();
    }

    public void addInterceptor(Interceptor interceptor)
    {
        this.messageInterceptors.add(interceptor);
    }

    public void removeInterceptor(Interceptor interceptor)
    {
        this.messageInterceptors.remove(interceptor);
    }

    public void clearInterceptors()
    {
        this.messageInterceptors.clear();
    }

    static int newMessageId()
    {
        return idGen.incrementAndGet();
    }

    /**
     * Sends a request message, setting the provided callback to be called with the response received.
     *
     * @param request the request to send.
     * @param callback the callback to set for the response to {@code request}.
     */
    public <Q> void send(Request<?, Q> request, MessageCallback<Q> callback)
    {
        if (request.isLocal())
        {
            deliverLocally(request, callback);
        }
        else
        {
            registerCallback(request, callback);
            ClientWarn.instance.storeForRequest(request.id());
            updateBackPressureOnSend(request);
            sendRequest(request, callback);
        }
    }

    /**
     * Sends the requests for a provided dispatcher, setting the provided callback to be called with received responses.
     *
     * @param dispatcher the dispatcher to use to generate requests to send.
     * @param callback the callback to set for responses to the request from {@code dispatcher}.
     */
    public <Q> void send(Request.Dispatcher<?, Q> dispatcher, MessageCallback<Q> callback)
    {
        assert callback != null && !dispatcher.verb().isOneWay();

        for (Request<?, Q> request : dispatcher.remoteRequests())
            send(request, callback);

        if (dispatcher.hasLocalRequest())
            deliverLocally(dispatcher.localRequest(), callback);
    }

    private <P, Q> void deliverLocally(Request<P, Q> request, MessageCallback<Q> callback)
    {
        Consumer<Response<Q>> handleResponse = response ->
                                               request.responseExecutor().execute(() -> deliverLocalResponse(request, response, callback),
                                                                                  ExecutorLocals.create());

        Consumer<Response<Q>> onResponse = messageInterceptors.isEmpty()
                                           ? handleResponse
                                           : r -> messageInterceptors.intercept(r, handleResponse, null);

        Runnable onAborted = () ->
        {
            Tracing.trace("Discarding partial local response (timed out)");
            MessagingService.instance().incrementDroppedMessages(request);
            callback.onTimeout(FBUtilities.getBroadcastAddress());
        };
        Consumer<Request<P, Q>> consumer = rq ->
        {
            if (rq.isTimedOut(ApproximateTime.currentTimeMillis()))
            {
                onAborted.run();
                return;
            }
            rq.execute(onResponse, onAborted);
        };
        deliverLocallyInternal(request, consumer, callback);
    }

    private <P, Q> void deliverLocalResponse(Request<P, Q> request, Response<Q> response, MessageCallback<Q> callback)
    {
        addLatency(request.verb(), request.to(), request.lifetimeMillis());
        response.deliverTo(callback);
    }

    private <P, Q> void registerCallback(Request<P, Q> request, MessageCallback<Q> callback)
    {
        long startTime = request.operationStartMillis();
        long timeout = request.timeoutMillis();
        TracingAwareExecutor executor = request.responseExecutor();

        for (Request.Forward forward : request.forwards())
            registerCallback(forward.id, forward.to, request.verb(), callback, startTime, timeout, executor);

        registerCallback(request.id(), request.to(), request.verb(), callback, startTime, timeout, executor);
    }

    private <Q> void registerCallback(int id,
                                      InetAddress to,
                                      Verb<?, Q> type,
                                      MessageCallback<Q> callback,
                                      long startTimeMillis,
                                      long timeout,
                                      TracingAwareExecutor executor)
    {
        CallbackInfo previous = callbacks.put(id, new CallbackInfo<>(to, callback, type, executor, startTimeMillis), timeout);
        assert previous == null : String.format("Callback already exists for id %d! (%s)", id, previous);
    }

    /**
     * Sends a request and returns a future on the reception of the response.
     *
     * @param request the request to send.
     * @return a future on the reception of the response payload to {@code request}. The future may either complete
     * sucessfully, complete exceptionally with an {@link InternalRequestExecutionException} if destination sends back
     * an error, or complete exceptionally with a {@link CallbackExpiredException} if no responses is received before
     * the timeout on the message.
     */
    public <Q> CompletableFuture<Q> sendSingleTarget(Request<?, Q> request)
    {
        SingleTargetCallback<Q> callback = new SingleTargetCallback<>();
        send(request, callback);
        return callback;
    }

    /**
     * Forwards a request to its final destination.
     * <p>
     * This doesn't set a callback since the final destination will respond to initial sender (which has set the proper
     * callbacks).
     */
    void forward(ForwardRequest<?, ?> request)
    {
        // Note that we don't attempt to intercept requests for which we're just the forwarder. Those will be intercepted
        // on the final receiver if needs be.
        sendInternal(request);
    }

    /**
     * Sends a one-way request.
     * <p>
     * Note that this is a fire-and-forget type of method: the method returns as soon as the request has been set to
     * be send and there is no way to know if the request has been successfully delivered or not.
     *
     * @param request the request to send.
     */
    public void send(OneWayRequest<?> request)
    {
        if (request.isLocal())
            deliverLocallyOneWay(request);
        else
            sendRequest(request, null);
    }

    /**
     * Sends the one-way requests generated by the provided dispatcher.
     * <p>
     * Note that this is a fire-and-forget type of method: the method returns as soon as the requets have been set to
     * be send and there is no way to know if the requests have been successfully delivered or not.
     *
     * @param dispatcher the dispatcher to use to generate the one-way requests to send.
     */
    public void send(OneWayRequest.Dispatcher<?> dispatcher)
    {
        for (OneWayRequest<?> request : dispatcher.remoteRequests())
            sendRequest(request, null);

        if (dispatcher.hasLocalRequest())
            deliverLocallyOneWay(dispatcher.localRequest());
    }

    private void deliverLocallyOneWay(OneWayRequest<?> request)
    {
        deliverLocallyInternal(request, r -> r.execute(r.verb().EMPTY_RESPONSE_CONSUMER, () -> {}), null);
    }

    /**
     * Sends a response message.
     */
    void reply(Response<?> response)
    {
        Tracing.trace("Enqueuing {} response to {}", response.verb(), response.from());
        sendResponse(response);
    }

    private <P, Q> void sendRequest(Request<P, Q> request, MessageCallback<Q> callback)
    {
        messageInterceptors.interceptRequest(request, this::sendInternal, callback);
    }

    private <Q> void sendResponse(Response<Q> response)
    {
        messageInterceptors.intercept(response, this::sendInternal, null);
    }

    private CompletableFuture<Void> sendInternal(Message<?> message)
    {
        if (logger.isTraceEnabled())
            logger.trace("Sending {}", message);

        // TODO Refactor getMessagingConnection to return CompletableFuture and change its usages accordingly.
//        OutboundMessagingPool outboundMessagingPool = getMessagingConnection(message.to());
//        if (outboundMessagingPool != null)
//            outboundMessagingPool.sendMessage(message, message.id());
        return getMessagingConnection(message.to()).thenAccept(mc -> mc.sendMessage(message, message.id()));
    }

    private <P, Q, M extends Request<P, Q>> void deliverLocallyInternal(M request,
                                                                        Consumer<M> consumer,
                                                                        MessageCallback<Q> callback)
    {
        messageInterceptors.interceptRequest(request,
                                             rq -> rq.requestExecutor().execute(() -> consumer.accept(rq), ExecutorLocals.create()),
                                             callback);
    }

    /**
     * Updates the back-pressure state on sending the provided message.
     *
     * @param request The request sent.
     */
    <Q> CompletableFuture<Void> updateBackPressureOnSend(Request<?, Q> request)
    {
        if (request.verb().supportsBackPressure() && DatabaseDescriptor.backPressureEnabled())
        {
            return getMessagingConnection(request.to()).thenAccept(mc -> {
                if (mc != null)
                {
                    BackPressureState backPressureState = mc.getBackPressureState();
                    if (backPressureState == null)
                        return;
                    backPressureState.onRequestSent(request);
                }
            });
        }

        return CompletableFuture.completedFuture(null);
    }

    /**
     * Updates the back-pressure state on reception from the given host if enabled and the given message callback supports it.
     *
     * @param host The replica host the back-pressure state refers to.
     * @param verb The message verb.
     * @param timeout True if updated following a timeout, false otherwise.
     */
    CompletableFuture<Void> updateBackPressureOnReceive(InetAddress host, Verb<?, ?> verb, boolean timeout)
    {
        if (verb.supportsBackPressure() && DatabaseDescriptor.backPressureEnabled())
        {
            return getMessagingConnection(host).thenAccept(mc -> {
                if (mc != null)
                {
                    BackPressureState backPressureState = mc.getBackPressureState();
                    if (backPressureState == null)
                        return;
                    if (!timeout)
                        backPressureState.onResponseReceived();
                    else
                        backPressureState.onResponseTimeout();
                }
            });
        }

        return CompletableFuture.completedFuture(null);
    }

    /**
     * Applies back-pressure for the given hosts, according to the configured strategy.
     *
     * If the local host is present, it is removed from the pool, as back-pressure is only applied
     * to remote hosts.
     *
     * @param hosts The hosts to apply back-pressure to.
     * @param timeoutInNanos The max back-pressure timeout.
     */
    @SuppressWarnings("unchecked")
    public CompletableFuture<Void> applyBackPressure(Iterable<InetAddress> hosts, long timeoutInNanos)
    {
        if (DatabaseDescriptor.backPressureEnabled())
        {
            Set<BackPressureState> states = new HashSet<BackPressureState>();
            CompletableFuture<Void> future = null;
            for (InetAddress host : hosts)
            {
                if (host.equals(FBUtilities.getBroadcastAddress()))
                    continue;

                CompletableFuture<Void> next = getMessagingConnection(host).thenAccept(mc -> {
                    if (mc != null)
                        states.add(mc.getBackPressureState());
                });

                if (future == null)
                    future = next;
                else
                    future = future.thenAcceptBoth(next, (a,b) -> {});
            }

            if (future != null)
            {
                return future.thenCompose(ignored -> backPressure.apply(states, timeoutInNanos, TimeUnit.NANOSECONDS));
            }
        }

        return CompletableFuture.completedFuture(null);
    }

    void markTimeout(InetAddress addr)
    {
        OutboundMessagingPool conn = channelManagers.get(addr);
        if (conn != null)
            conn.incrementTimeout();
    }

    /**
     * Track latency information for the dynamic snitch.
     *
     * @param verb the verb for which we're adding latency information.
     * @param address the host that replied to the message for which we're adding latency.
     * @param latency the latency to record in milliseconds
     */
    void addLatency(Verb<?, ?> verb, InetAddress address, long latency)
    {
        for (ILatencySubscriber subscriber : subscribers)
            subscriber.receiveTiming(verb, address, latency);
    }

    /**
     * called from gossiper when it notices a node is not responding.
     */
    public CompletableFuture<Void> convict(InetAddress address)
    {
        OutboundMessagingPool messagingPool = channelManagers.remove(address);
        if (messagingPool != null)
        {
            return CompletableFuture.runAsync(() -> { messagingPool.close(false); });
        }
        return CompletableFuture.completedFuture(null);
    }

    public void listen()
    {
        callbacks.reset(); // hack to allow tests to stop/restart MS
        listen(FBUtilities.getLocalAddress());
        if (shouldListenOnBroadcastAddress())
            listen(FBUtilities.getBroadcastAddress());
        listenGate.signalAll();
    }

    public static boolean shouldListenOnBroadcastAddress()
    {
        return DatabaseDescriptor.shouldListenOnBroadcastAddress()
               && !FBUtilities.getLocalAddress().equals(FBUtilities.getBroadcastAddress());
    }

    /**
     * Listen on the specified port.
     *
     * @param localEp InetAddress whose port to listen on.
     */
    private void listen(InetAddress localEp) throws ConfigurationException
    {
        IInternodeAuthenticator authenticator = DatabaseDescriptor.getInternodeAuthenticator();
        int receiveBufferSize = DatabaseDescriptor.getInternodeRecvBufferSize();

        if (DatabaseDescriptor.getServerEncryptionOptions().internode_encryption != ServerEncryptionOptions.InternodeEncryption.none)
        {
            InetSocketAddress localAddr = new InetSocketAddress(localEp, DatabaseDescriptor.getSSLStoragePort());
            ChannelGroup channelGroup = new DefaultChannelGroup("EncryptedInternodeMessagingGroup", NettyFactory.executorForChannelGroups());
            InboundInitializer initializer = new InboundInitializer(authenticator, DatabaseDescriptor.getServerEncryptionOptions(), channelGroup);
            Channel encryptedChannel = NettyFactory.instance.createInboundChannel(localAddr, initializer, receiveBufferSize);
            serverChannels.add(new ServerChannel(encryptedChannel, channelGroup));
        }

        if (DatabaseDescriptor.getServerEncryptionOptions().internode_encryption != ServerEncryptionOptions.InternodeEncryption.all)
        {
            InetSocketAddress localAddr = new InetSocketAddress(localEp, DatabaseDescriptor.getStoragePort());
            ChannelGroup channelGroup = new DefaultChannelGroup("InternodeMessagingGroup", NettyFactory.executorForChannelGroups());
            InboundInitializer initializer = new InboundInitializer(authenticator, null, channelGroup);
            Channel channel = NettyFactory.instance.createInboundChannel(localAddr, initializer, receiveBufferSize);
            serverChannels.add(new ServerChannel(channel, channelGroup));
        }

        if (serverChannels.isEmpty())
            throw new IllegalStateException("no listening channels set up in MessagingService!");
    }

    /**
     * A simple struct to wrap up the the components needed for each listening socket.
     */
    @VisibleForTesting
    static class ServerChannel
    {
        /**
         * The base {@link Channel} that is doing the spcket listen/accept.
         */
        private final Channel channel;

        /**
         * A group of the open, inbound {@link Channel}s connected to this node. This is mostly interesting so that all of
         * the inbound connections/channels can be closed when the listening socket itself is being closed.
         */
        private final ChannelGroup connectedChannels;

        private ServerChannel(Channel channel, ChannelGroup channelGroup)
        {
            this.channel = channel;
            this.connectedChannels = channelGroup;
        }

        void close()
        {
            channel.close().syncUninterruptibly();
            connectedChannels.close().syncUninterruptibly();
        }
        int size()

        {
            return connectedChannels.size();
        }
    }

    public void waitUntilListening()
    {
        try
        {
            listenGate.await();
        }
        catch (InterruptedException ie)
        {
            logger.trace("await interrupted");
        }
    }

    public boolean isListening()
    {
        return listenGate.isSignaled();
    }

    public void destroyConnectionPool(InetAddress to)
    {
        OutboundMessagingPool pool = channelManagers.remove(to);
        if (pool != null)
            pool.close(true);
    }

    /**
     * Reconnect to the peer using the given {@code addr}. Outstanding messages in each channel will be sent on the
     * current channel. Typically this function is used for something like EC2 public IP addresses which need to be used
     * for communication between EC2 regions.
     *
     * @param address IP Address to identify the peer
     * @param preferredAddress IP Address to use (and prefer) going forward for connecting to the peer
     */
    public void reconnectWithNewIp(InetAddress address, InetAddress preferredAddress)
    {
        SystemKeyspace.updatePreferredIP(address, preferredAddress);

        OutboundMessagingPool messagingPool = channelManagers.get(address);
        if (messagingPool != null)
            messagingPool.reconnectWithNewIp(new InetSocketAddress(preferredAddress, portFor(address)));
    }

    private void reset(InetAddress address)
    {
        OutboundMessagingPool messagingPool = channelManagers.remove(address);
        if (messagingPool != null)
            messagingPool.close(false);
    }

    // TODO Figure out if this should be made async as well
    public InetAddress getCurrentEndpoint(InetAddress publicAddress)
    {
        try
        {
            OutboundMessagingPool messagingPool = getMessagingConnection(publicAddress).get();
            return messagingPool != null ? messagingPool.getPreferredRemoteAddr().getAddress() : null;
        }
        catch (InterruptedException | ExecutionException e)
        {
            return null;
        }
    }

    public void register(ILatencySubscriber subcriber)
    {
        subscribers.add(subcriber);
    }

    public void clearCallbacksUnsafe()
    {
        callbacks.reset();
    }

    /**
     * Wait for callbacks and don't allow any more to be created (since they could require writing hints)
     */
    public void shutdown()
    {
        logger.info("Waiting for messaging service to quiesce");
        // We may need to schedule hints, so it's erroneous to shut down the HINTS first
        assert !StageManager.getStage(Stage.HINTS).isShutdown();

        // the important part
        if (!callbacks.shutdownBlocking())
            logger.warn("Failed to wait for messaging service callbacks shutdown");

        // attempt to humor tests that try to stop and restart MS
        try
        {
            // first close the recieve channels
            for (ServerChannel serverChannel : serverChannels)
                serverChannel.close();

            // now close the send channels
            for (OutboundMessagingPool pool : channelManagers.values())
                pool.close(false);

            NettyFactory.instance.close();
        }
        catch (Exception e)
        {
            throw new IOError(e);
        }
    }

    public void receive(Message<?> message)
    {
        messageInterceptors.intercept(message, this::receiveInternal, this::reply);
    }

    private void receiveInternal(Message<?> message)
    {
        TraceState state = Tracing.instance.initializeFromMessage(message);
        if (state != null)
            state.trace("{} message received from {}", message.verb(), message.from());

        ExecutorLocals locals = ExecutorLocals.create(state, ClientWarn.instance.getForMessage(message.id()));
        if (message.isRequest())
            receiveRequestInternal((Request<?, ?>) message, locals);
        else
            receiveResponseInternal((Response<?>) message, locals);
    }

    private <P, Q> void receiveRequestInternal(Request<P, Q> request, ExecutorLocals locals)
    {
        request.requestExecutor().execute(MessageDeliveryTask.forRequest(request), locals);
    }

    private <Q> void receiveResponseInternal(Response<Q> response, ExecutorLocals locals)
    {
        CallbackInfo<Q> info = getRegisteredCallback(response, false);
        // Ignore expired callback info (we already logged in getRegisteredCallback)
        if (info != null)
            info.responseExecutor.execute(MessageDeliveryTask.forResponse(response), locals);
    }

    // Only required by legacy serialization. Can inline in following metho when we get rid of that.
    CallbackInfo<?> getRegisteredCallback(int id, boolean remove, InetAddress from)
    {
        ExpiringMap.CacheableObject<CallbackInfo<?>> cObj = remove ? callbacks.remove(id) : callbacks.get(id);
        if (cObj == null)
        {
            String msg = "Callback already removed for message {} from {}, ignoring response";
            logger.trace(msg, id, from);
            Tracing.trace(msg, id, from);
            return null;
        }
        else
        {
            return cObj.get();
        }
    }

    @SuppressWarnings("unchecked")
    <Q> CallbackInfo<Q> getRegisteredCallback(Response<Q> response, boolean remove)
    {
        return (CallbackInfo<Q>) getRegisteredCallback(response.id(), remove, response.from());
    }

    public static void validateMagic(int magic) throws IOException
    {
        if (magic != PROTOCOL_MAGIC)
            throw new IOException("invalid protocol header");
    }

    public static int getBits(int packed, int start, int count)
    {
        return (packed >>> ((start + 1) - count)) & ~(-1 << count);
    }

    /**
     * @return the last version associated with address, or @param version if this is the first such version
     */
    public MessagingVersion setVersion(InetAddress endpoint, MessagingVersion version)
    {
        logger.trace("Setting version {} for {}", version, endpoint);

        MessagingVersion v = versions.put(endpoint, version);
        return v == null ? version : v;
    }

    public void resetVersion(InetAddress endpoint)
    {
        logger.trace("Resetting version for {}", endpoint);
        versions.remove(endpoint);
    }

    /**
     * Returns the messaging-version as announced by the given node but capped
     * to the min of the version as announced by the node and {@link #current_version}.
     */
    public MessagingVersion getVersion(InetAddress endpoint)
    {
        MessagingVersion v = versions.get(endpoint);
        if (v == null)
        {
            // we don't know the version. assume current. we'll know soon enough if that was incorrect.
            //logger.trace("Assuming current protocol version for {}", endpoint);
            return MessagingService.current_version;
        }
        else
            return MessagingVersion.min(v, MessagingService.current_version);
    }

    @Deprecated
    public int getVersion(String endpoint) throws UnknownHostException
    {
        return getVersion(InetAddress.getByName(endpoint)).protocolVersion().handshakeVersion;
    }

    /**
     * Returns the messaging-version exactly as announced by the given endpoint.
     */
    public MessagingVersion getRawVersion(InetAddress endpoint)
    {
        MessagingVersion v = versions.get(endpoint);
        if (v == null)
            throw new IllegalStateException("getRawVersion() was called without checking knowsVersion() result first");
        return v;
    }

    public boolean knowsVersion(InetAddress endpoint)
    {
        return versions.containsKey(endpoint);
    }

    void incrementDroppedMessages(Message<?> message)
    {
        Verb<?, ?> definition = message.verb();
        assert !definition.isOneWay() : "Shouldn't drop a one-way message";
        if (message.isRequest())
        {
            Object payload = message.payload();
            if (payload instanceof IMutation)
                updateDroppedMutationCount((IMutation) payload);
        }

        droppedMessages.onDroppedMessage(message);
    }


    private void updateDroppedMutationCount(IMutation mutation)
    {
        assert mutation != null : "Mutation should not be null when updating dropped mutations count";

        for (TableId tableId : mutation.getTableIds())
        {
            ColumnFamilyStore cfs = Keyspace.open(mutation.getKeyspaceName()).getColumnFamilyStore(tableId);
            if (cfs != null)
            {
                cfs.metric.droppedMutations.inc();
            }
        }
    }



    private static void handleIOExceptionOnClose(IOException e) throws IOException
    {
        // dirty hack for clean shutdown on OSX w/ Java >= 1.8.0_20
        // see https://bugs.openjdk.java.net/browse/JDK-8050499;
        // also CASSANDRA-12513
        if (FBUtilities.isMacOSX)
        {
            switch (e.getMessage())
            {
                case "Unknown error: 316":
                case "No such file or directory":
                    return;
            }
        }

        throw e;
    }

    public Map<String, Integer> getLargeMessagePendingTasks()
    {
        Map<String, Integer> pendingTasks = new HashMap<String, Integer>(channelManagers.size());
        for (Map.Entry<InetAddress, OutboundMessagingPool> entry : channelManagers.entrySet())
            pendingTasks.put(entry.getKey().getHostAddress(), entry.getValue().largeMessageChannel.getPendingMessages());
        return pendingTasks;
    }

    public Map<String, Long> getLargeMessageCompletedTasks()
    {
        Map<String, Long> completedTasks = new HashMap<String, Long>(channelManagers.size());
        for (Map.Entry<InetAddress, OutboundMessagingPool> entry : channelManagers.entrySet())
            completedTasks.put(entry.getKey().getHostAddress(), entry.getValue().largeMessageChannel.getCompletedMessages());
        return completedTasks;
    }

    public Map<String, Long> getLargeMessageDroppedTasks()
    {
        Map<String, Long> droppedTasks = new HashMap<String, Long>(channelManagers.size());
        for (Map.Entry<InetAddress, OutboundMessagingPool> entry : channelManagers.entrySet())
            droppedTasks.put(entry.getKey().getHostAddress(), entry.getValue().largeMessageChannel.getDroppedMessages());
        return droppedTasks;
    }

    public Map<String, Integer> getSmallMessagePendingTasks()
    {
        Map<String, Integer> pendingTasks = new HashMap<String, Integer>(channelManagers.size());
        for (Map.Entry<InetAddress, OutboundMessagingPool> entry : channelManagers.entrySet())
            pendingTasks.put(entry.getKey().getHostAddress(), entry.getValue().smallMessageChannel.getPendingMessages());
        return pendingTasks;
    }

    public Map<String, Long> getSmallMessageCompletedTasks()
    {
        Map<String, Long> completedTasks = new HashMap<String, Long>(channelManagers.size());
        for (Map.Entry<InetAddress, OutboundMessagingPool> entry : channelManagers.entrySet())
            completedTasks.put(entry.getKey().getHostAddress(), entry.getValue().smallMessageChannel.getCompletedMessages());
        return completedTasks;
    }

    public Map<String, Long> getSmallMessageDroppedTasks()
    {
        Map<String, Long> droppedTasks = new HashMap<String, Long>(channelManagers.size());
        for (Map.Entry<InetAddress, OutboundMessagingPool> entry : channelManagers.entrySet())
            droppedTasks.put(entry.getKey().getHostAddress(), entry.getValue().smallMessageChannel.getDroppedMessages());
        return droppedTasks;
    }

    public Map<String, Integer> getGossipMessagePendingTasks()
    {
        Map<String, Integer> pendingTasks = new HashMap<String, Integer>(channelManagers.size());
        for (Map.Entry<InetAddress, OutboundMessagingPool> entry : channelManagers.entrySet())
            pendingTasks.put(entry.getKey().getHostAddress(), entry.getValue().gossipChannel.getPendingMessages());
        return pendingTasks;
    }

    public Map<String, Long> getGossipMessageCompletedTasks()
    {
        Map<String, Long> completedTasks = new HashMap<String, Long>(channelManagers.size());
        for (Map.Entry<InetAddress, OutboundMessagingPool> entry : channelManagers.entrySet())
            completedTasks.put(entry.getKey().getHostAddress(), entry.getValue().gossipChannel.getCompletedMessages());
        return completedTasks;
    }

    public Map<String, Long> getGossipMessageDroppedTasks()
    {
        Map<String, Long> droppedTasks = new HashMap<String, Long>(channelManagers.size());
        for (Map.Entry<InetAddress, OutboundMessagingPool> entry : channelManagers.entrySet())
            droppedTasks.put(entry.getKey().getHostAddress(), entry.getValue().gossipChannel.getDroppedMessages());
        return droppedTasks;
    }

    public Map<String, Integer> getDroppedMessages()
    {
        return droppedMessages.getSnapshot();
    }

    /**
     * @return Dropped message metrics by group with full access to all underlying metrics.
     * Used by DSE's DroppedMessagesWriter
     */
    public Map<DroppedMessages.Group, DroppedMessageMetrics> getDroppedMessagesWithAllMetrics()
    {
        return droppedMessages.getAllMetrics();
    }

    public long getTotalTimeouts()
    {
        return ConnectionMetrics.totalTimeouts.getCount();
    }

    public Map<String, Long> getTimeoutsPerHost()
    {
        Map<String, Long> result = new HashMap<String, Long>(channelManagers.size());
        for (Map.Entry<InetAddress, OutboundMessagingPool> entry : channelManagers.entrySet())
        {
            String ip = entry.getKey().getHostAddress();
            long recent = entry.getValue().getTimeouts();
            result.put(ip, recent);
        }
        return result;
    }

    public Map<String, Double> getBackPressurePerHost()
    {
        Map<String, Double> map = new HashMap<>(channelManagers.size());
        for (Map.Entry<InetAddress, OutboundMessagingPool> entry : channelManagers.entrySet())
            map.put(entry.getKey().getHostAddress(), entry.getValue().getBackPressureState().getBackPressureRateLimit());

        return map;
    }

    @Override
    public void setBackPressureEnabled(boolean enabled)
    {
        DatabaseDescriptor.setBackPressureEnabled(enabled);
    }

    @Override
    public boolean isBackPressureEnabled()
    {
        return DatabaseDescriptor.backPressureEnabled();
    }

    public static IPartitioner globalPartitioner()
    {
        return StorageService.instance.getTokenMetadata().partitioner;
    }

    public static void validatePartitioner(Collection<? extends AbstractBounds<?>> allBounds)
    {
        for (AbstractBounds<?> bounds : allBounds)
            validatePartitioner(bounds);
    }

    public static void validatePartitioner(AbstractBounds<?> bounds)
    {
        if (globalPartitioner() != bounds.left.getPartitioner())
            throw new AssertionError(String.format("Partitioner in bounds serialization. Expected %s, was %s.",
                                                   globalPartitioner().getClass().getName(),
                                                   bounds.left.getPartitioner().getClass().getName()));
    }

    CompletableFuture<OutboundMessagingPool> getMessagingConnection(InetAddress to)
    {
        OutboundMessagingPool existingPool = channelManagers.get(to);
        if (existingPool != null)
        {
            return CompletableFuture.completedFuture(existingPool);
        }

        final boolean secure = isEncryptedConnection(to);
        final int port = portFor(secure);
        if (!DatabaseDescriptor.getInternodeAuthenticator().authenticate(to, port))
            return CompletableFuture.completedFuture(null);

        return CompletableFuture.supplyAsync(() ->
                                             {
                                                 InetSocketAddress preferredRemote = new InetSocketAddress(SystemKeyspace.getPreferredIP(to), port);
                                                 InetSocketAddress local = new InetSocketAddress(FBUtilities.getLocalAddress(), 0);
                                                 ServerEncryptionOptions encryptionOptions = secure ? DatabaseDescriptor.getServerEncryptionOptions() : null;
                                                 IInternodeAuthenticator authenticator = DatabaseDescriptor.getInternodeAuthenticator();

                                                 OutboundMessagingPool pool = new OutboundMessagingPool(preferredRemote,
                                                                                                        local,
                                                                                                        encryptionOptions,
                                                                                                        backPressure.newState(to),
                                                                                                        authenticator);
                                                 OutboundMessagingPool existing = channelManagers.putIfAbsent(to, pool);
                                                 if (existing != null)
                                                 {
                                                     pool.close(false);
                                                     pool = existing;
                                                 }
                                                 return pool;
                                             });

    }

    BackPressureState getBackPressureState(InetAddress host)
     {
         OutboundMessagingPool messagingPool = null;
         try
         {
             messagingPool = getMessagingConnection(host).get();
         }
         catch (InterruptedException | ExecutionException e)
         {
             return null;
         }
         return messagingPool != null ? messagingPool.getBackPressureState() : null;
     }

    public static int portFor(InetAddress addr)
    {
        final boolean secure = isEncryptedConnection(addr);
        return portFor(secure);
    }

    private static int portFor(boolean secure)
    {
        return secure ? DatabaseDescriptor.getSSLStoragePort() : DatabaseDescriptor.getStoragePort();
    }

    @VisibleForTesting
    boolean isConnected(InetAddress address, Message<?> message)
    {
        OutboundMessagingPool pool = channelManagers.get(address);
        if (pool == null)
            return false;
        return pool.getConnection(message).isConnected();
    }

    public static boolean isEncryptedConnection(InetAddress address)
    {
        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        switch (DatabaseDescriptor.getServerEncryptionOptions().internode_encryption)
        {
            case none:
                return false; // if nothing needs to be encrypted then return immediately.
            case all:
                break;
            case dc:
                if (snitch.getDatacenter(address).equals(snitch.getDatacenter(FBUtilities.getBroadcastAddress())))
                    return false;
                break;
            case rack:
                // for rack then check if the DC's are the same.
                if (snitch.getRack(address).equals(snitch.getRack(FBUtilities.getBroadcastAddress()))
                    && snitch.getDatacenter(address).equals(snitch.getDatacenter(FBUtilities.getBroadcastAddress())))
                    return false;
                break;
        }
        return true;
    }
}
