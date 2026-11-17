package dev.yumix.rpc.client;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import dev.yumix.rpc.RpcMethodDescriptor;
import dev.yumix.rpc.protocol.NsHeadFrame;
import dev.yumix.rpc.protocol.NsHeadFrameDecoder;
import dev.yumix.rpc.protocol.NsHeadFrameEncoder;
import dev.yumix.rpc.protocol.RpcResponse;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.ScheduledFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/** Application-scoped pool of persistent, multiplexed RPC connections. */
public final class RpcClientPool implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(RpcClientPool.class);

    private final Object lock = new Object();
    private final InetSocketAddress address;
    private final String provider;
    private final Builder options;
    private final EventLoopGroup group;
    private final Bootstrap bootstrap;
    private final Slot[] slots;
    private final ArrayDeque<Call<?>> queued = new ArrayDeque<>();
    private final List<CompletableFuture<Void>> readyWaiters = new ArrayList<>();
    private final List<Consumer<RpcConnectionEvent>> listeners = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final AtomicLong nextRequestId = new AtomicLong(ThreadLocalRandom.current().nextLong());
    private final AtomicLong reconnectAttempts = new AtomicLong();
    private final AtomicLong heartbeatTimeouts = new AtomicLong();
    private final AtomicLong connectionWaitTimeouts = new AtomicLong();
    private final AtomicLong responseTimeouts = new AtomicLong();
    private final AtomicLong discardedLateResponses = new AtomicLong();
    private volatile RpcPoolState state = RpcPoolState.STARTING;
    private volatile int readyConnections;
    private volatile int queuedCount;
    private long queuedBytes;
    private int consecutiveFailures;
    private int dialFailures;
    private boolean connecting;
    private volatile boolean closed;
    private ScheduledFuture<?> connectTask;

    public static Builder builder(InetSocketAddress address, String provider) {
        return new Builder(address, provider);
    }

    private RpcClientPool(Builder options) {
        this.options = options.copy();
        this.address = options.address;
        this.provider = options.provider;
        this.group = new NioEventLoopGroup(options.resolvedIoThreads(), new DefaultThreadFactory("rpc-pool-io"));
        this.bootstrap = new Bootstrap().group(group).channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) options.connectTimeoutMillis);
        this.slots = new Slot[options.connections];
        for (int i = 0; i < slots.length; i++) slots[i] = new Slot(options.maxInFlightPerConnection);
        scheduleConnect(0);
    }

    public <Req extends Message, Resp extends Message> CompletableFuture<Resp> call(
            RpcMethodDescriptor<Req, Resp> method, Req request) {
        return call(method, request, 0);
    }

    public <Req extends Message, Resp extends Message> CompletableFuture<Resp> call(
            RpcMethodDescriptor<Req, Resp> method, Req request, long logId) {
        Objects.requireNonNull(method, "method");
        return request(method.methodId(), logId, request, method.responseParser());
    }

    public <Resp extends Message> CompletableFuture<Resp> request(
            long methodId, long logId, Message request, Parser<Resp> responseParser) {
        if (methodId <= NsHeadFrame.PING_METHOD_ID || methodId >= NsHeadFrame.PONG_METHOD_ID) {
            throw new IllegalArgumentException("methodId is reserved for heartbeat control frames");
        }
        if ((logId & ~0xffff_ffffL) != 0) throw new IllegalArgumentException("logId must fit in uint32");
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(responseParser, "responseParser");
        byte[] body = request.toByteArray();
        if (body.length > options.maxBodyLength) throw new IllegalArgumentException("Request body exceeds limit");
        Call<Resp> call = new Call<>(methodId, logId, responseParser, body);
        call.future.whenComplete((response, error) -> {
            if (call.future.isCancelled()) cancel(call);
        });
        if (closed) return failed(call, new RpcException("RPC pool is closed", null));

        Selection selection = selectReady();
        if (selection != null) {
            send(call, selection);
            return call.future;
        }
        synchronized (lock) {
            if (closed) return failed(call, new RpcException("RPC pool is closed", null));
            if (readyConnections > 0) {
                return failed(call, new LocalOverloadException("All RPC connections are at their in-flight limit"));
            }
            if (state == RpcPoolState.UNAVAILABLE) {
                return failed(call, new ConnectionUnavailableException("RPC reconnect failure limit reached"));
            }
            if (queued.size() >= options.maxQueuedCalls
                    || body.length > options.maxQueuedBytes - queuedBytes) {
                return failed(call, new LocalOverloadException("RPC connection wait queue is full"));
            }
            queued.addLast(call);
            queuedCount++;
            queuedBytes += body.length;
            call.waitTimeout = group.next().schedule(() -> timeoutQueued(call),
                    options.connectionWaitTimeoutMillis, TimeUnit.MILLISECONDS);
        }
        drainQueue();
        return call.future;
    }

    public CompletableFuture<Void> ready() {
        synchronized (lock) {
            if (closed) return CompletableFuture.failedFuture(new RpcException("RPC pool is closed", null));
            if (readyConnections > 0) return CompletableFuture.completedFuture(null);
            if (state == RpcPoolState.UNAVAILABLE) {
                return CompletableFuture.failedFuture(new ConnectionUnavailableException(
                        "RPC reconnect failure limit reached"));
            }
            CompletableFuture<Void> future = new CompletableFuture<>();
            readyWaiters.add(future);
            future.whenComplete((result, error) -> {
                if (future.isCancelled()) {
                    synchronized (lock) { readyWaiters.remove(future); }
                }
            });
            return future;
        }
    }

    public RpcPoolState state() {
        return state;
    }

    public RpcPoolStats stats() {
        int queueSize;
        long queueBytes;
        int failures;
        synchronized (lock) {
            queueSize = queued.size();
            queueBytes = queuedBytes;
            failures = consecutiveFailures;
        }
        int inFlight = 0;
        List<RpcPoolStats.Connection> connections = new ArrayList<>();
        for (Slot slot : slots) {
            ConnectionRef connection = slot.connection;
            if (connection != null && connection.channel.isActive()) {
                Channel channel = connection.channel;
                connections.add(new RpcPoolStats.Connection(channel.id().asShortText(),
                        channel.localAddress().toString(), channel.remoteAddress().toString(),
                        slot.inFlight.get()));
            }
            inFlight += slot.inFlight.get();
        }
        return new RpcPoolStats(state, readyConnections, queueSize, queueBytes, inFlight, failures,
                reconnectAttempts.get(), heartbeatTimeouts.get(), connectionWaitTimeouts.get(),
                responseTimeouts.get(), discardedLateResponses.get(), connections);
    }

    public void addStateListener(Consumer<RpcConnectionEvent> listener) {
        listeners.add(Objects.requireNonNull(listener, "listener"));
    }

    private Selection selectReady() {
        int start = ThreadLocalRandom.current().nextInt(slots.length);
        Slot best = null;
        int fewest = Integer.MAX_VALUE;
        for (int i = 0; i < slots.length; i++) {
            Slot slot = slots[(start + i) % slots.length];
            ConnectionRef connection = slot.connection;
            int current = slot.inFlight.get();
            if (connection != null && connection.channel.isActive() && current < fewest) {
                best = slot;
                fewest = current;
            }
        }
        if (best != null) {
            ConnectionRef connection = best.connection;
            if (connection != null && connection.channel.isActive() && best.permits.tryAcquire()) {
                best.inFlight.incrementAndGet();
                return new Selection(best, connection);
            }
        }
        for (int i = 0; i < slots.length; i++) {
            Slot slot = slots[(start + i) % slots.length];
            if (slot == best) continue;
            ConnectionRef connection = slot.connection;
            if (connection != null && connection.channel.isActive() && slot.permits.tryAcquire()) {
                slot.inFlight.incrementAndGet();
                return new Selection(slot, connection);
            }
        }
        return null;
    }

    private <Resp extends Message> void send(Call<Resp> call, Selection selection) {
        Slot slot = selection.slot;
        ConnectionRef connection = selection.connection;
        Channel channel = connection.channel;
        if (call.future.isCancelled() || closed || slot.connection != connection || !channel.isActive()) {
            release(slot);
            if (!call.future.isCancelled()) finish(call, new OutcomeUnknownException("RPC connection closed before write"));
            return;
        }
        long requestId = nextId();
        call.requestId = requestId;
        call.slot = slot;
        call.connection = connection;
        connection.pending.put(requestId, call);
        if (call.future.isCancelled() || slot.connection != connection || !channel.isActive()) {
            if (connection.pending.remove(requestId, call)) release(slot);
            if (!call.future.isCancelled()) finish(call, new OutcomeUnknownException("RPC connection closed before write"));
            return;
        }
        try {
            NsHeadFrame frame = new NsHeadFrame((int) requestId & 0xffff,
                    NsHeadFrame.PROTOCOL_VERSION, call.logId, provider, call.methodId, requestId, call.body);
            channel.writeAndFlush(frame).addListener(write -> {
                if (!write.isSuccess()) {
                    if (connection.pending.remove(requestId, call)) {
                        release(slot);
                        finish(call, new OutcomeUnknownException("RPC write failed; request outcome is unknown"));
                    }
                } else if (connection.pending.get(requestId) == call) {
                    call.responseTimeout = channel.eventLoop().schedule(() -> {
                        if (connection.pending.remove(requestId, call)) {
                            release(slot);
                            responseTimeouts.incrementAndGet();
                            finish(call, new RpcTimeoutException(RpcTimeoutException.Stage.RESPONSE,
                                    "RPC response timed out after " + options.responseTimeoutMillis + " ms"));
                        }
                    }, options.responseTimeoutMillis, TimeUnit.MILLISECONDS);
                    if (connection.pending.get(requestId) != call) call.cancelResponseTimeout();
                }
            });
        } catch (RuntimeException e) {
            if (connection.pending.remove(requestId, call)) {
                release(slot);
                call.cancelResponseTimeout();
                finish(call, new RpcException("RPC write could not be scheduled", e));
            }
        }
    }

    private void drainQueue() {
        while (true) {
            Call<?> call;
            Selection selection;
            synchronized (lock) {
                call = queued.peekFirst();
                if (call == null) return;
                selection = selectReady();
                if (selection == null) return;
                queued.removeFirst();
                queuedCount--;
                queuedBytes -= call.body.length;
                call.cancelWaitTimeout();
            }
            send(call, selection);
        }
    }

    private void timeoutQueued(Call<?> call) {
        synchronized (lock) {
            if (!queued.remove(call)) return;
            queuedCount--;
            queuedBytes -= call.body.length;
        }
        connectionWaitTimeouts.incrementAndGet();
        finish(call, new RpcTimeoutException(RpcTimeoutException.Stage.CONNECTION_WAIT,
                "RPC connection wait timed out after " + options.connectionWaitTimeoutMillis + " ms"));
    }

    private void cancel(Call<?> call) {
        synchronized (lock) {
            if (queued.remove(call)) {
                queuedCount--;
                queuedBytes -= call.body.length;
                call.cancelWaitTimeout();
                return;
            }
        }
        Slot slot = call.slot;
        ConnectionRef connection = call.connection;
        if (slot != null && connection != null && connection.pending.remove(call.requestId, call)) {
            release(slot);
            call.cancelResponseTimeout();
        }
    }

    private static <T extends Message> CompletableFuture<T> failed(Call<T> call, RuntimeException error) {
        call.future.completeExceptionally(error);
        return call.future;
    }

    private void finish(Call<?> call, Throwable error) {
        complete(() -> call.future.completeExceptionally(error));
    }

    private <T extends Message> void finish(Call<T> call, T response) {
        complete(() -> call.future.complete(response));
    }

    private void complete(Runnable completion) {
        try {
            options.completionExecutor.execute(completion);
        } catch (RuntimeException e) {
            ForkJoinPool.commonPool().execute(completion);
        }
    }

    private void release(Slot slot) {
        slot.inFlight.decrementAndGet();
        slot.permits.release();
        if (queuedCount > 0) drainQueue();
    }

    private long nextId() {
        long id;
        do { id = nextRequestId.incrementAndGet(); } while (id == 0);
        return id;
    }

    private void scheduleConnect(long delayMillis) {
        synchronized (lock) {
            if (closed || connecting || !hasMissingSlot()) return;
            if (connectTask != null && !connectTask.isDone()) {
                if (delayMillis != 0 || readyConnections > 0) return;
                connectTask.cancel(false);
            }
            connectTask = group.next().schedule(this::attemptConnect,
                    delayMillis, TimeUnit.MILLISECONDS);
        }
    }

    private boolean hasMissingSlot() {
        for (Slot slot : slots) if (slot.connection == null) return true;
        return false;
    }

    private void attemptConnect() {
        Slot target = null;
        synchronized (lock) {
            connectTask = null;
            if (closed || connecting) return;
            for (Slot slot : slots) {
                if (slot.connection == null) { target = slot; break; }
            }
            if (target == null) return;
            connecting = true;
        }
        Slot slot = target;
        reconnectAttempts.incrementAndGet();
        Bootstrap attempt = bootstrap.clone().handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel socket) {
                ConnectionRef connection = new ConnectionRef(socket);
                socket.pipeline().addLast(new NsHeadFrameDecoder(options.maxBodyLength));
                socket.pipeline().addLast(new NsHeadFrameEncoder());
                socket.pipeline().addLast(new IdleStateHandler(
                        options.heartbeatIdleMillis, 0, 0, TimeUnit.MILLISECONDS));
                socket.pipeline().addLast(new PoolHandler(slot, connection));
            }
        });
        attempt.connect(address).addListener((ChannelFutureListener) future -> {
            List<Call<?>> failedCalls = List.of();
            List<CompletableFuture<Void>> failedWaiters = List.of();
            long delay = 0;
            synchronized (lock) {
                connecting = false;
                if (!future.isSuccess() && !closed) {
                    dialFailures++;
                    if (readyConnections == 0) {
                        consecutiveFailures++;
                        state = consecutiveFailures >= options.maxReconnectAttempts
                                ? RpcPoolState.UNAVAILABLE : RpcPoolState.RECONNECTING;
                        if (state == RpcPoolState.UNAVAILABLE) {
                            failedCalls = new ArrayList<>(queued);
                            queued.clear();
                            queuedCount = 0;
                            queuedBytes = 0;
                            failedWaiters = new ArrayList<>(readyWaiters);
                            readyWaiters.clear();
                        }
                    }
                    delay = backoffMillis(dialFailures);
                } else if (future.isSuccess()) {
                    dialFailures = 0;
                }
            }
            for (Call<?> call : failedCalls) {
                call.cancelWaitTimeout();
                finish(call, new ConnectionUnavailableException("RPC reconnect failed "
                        + options.maxReconnectAttempts + " times"));
            }
            for (CompletableFuture<Void> waiter : failedWaiters) {
                complete(() -> waiter.completeExceptionally(new ConnectionUnavailableException(
                        "RPC reconnect failure limit reached")));
            }
            if (!future.isSuccess() && !closed) {
                LOG.warn("RPC reconnect failed remote={} failures={} reason={}",
                        address, consecutiveFailures, future.cause().toString());
                emit(null, "connect failed");
            }
            scheduleConnect(delay);
        });
    }

    private long backoffMillis(int failures) {
        long base = options.reconnectInitialMillis;
        for (int i = 1; i < failures && base < options.reconnectMaxMillis; i++) {
            base = Math.min(options.reconnectMaxMillis, base >= options.reconnectMaxMillis / 2
                    ? options.reconnectMaxMillis : base * 2);
        }
        return Math.max(1, Math.min(options.reconnectMaxMillis,
                (long) (base * ThreadLocalRandom.current().nextDouble(0.8, 1.2))));
    }

    private void onConnected(Slot slot, ConnectionRef connection) {
        List<CompletableFuture<Void>> waiters;
        Channel channel = connection.channel;
        synchronized (lock) {
            if (closed) { channel.close(); return; }
            if (slot.connection == connection) return;
            slot.connection = connection;
            readyConnections++;
            consecutiveFailures = 0;
            dialFailures = 0;
            state = readyConnections == slots.length ? RpcPoolState.READY : RpcPoolState.DEGRADED;
            waiters = new ArrayList<>(readyWaiters);
            readyWaiters.clear();
        }
        LOG.info("RPC pool connected local={} remote={} connection={}",
                channel.localAddress(), channel.remoteAddress(), channel.id().asShortText());
        for (CompletableFuture<Void> waiter : waiters) complete(() -> waiter.complete(null));
        emit(channel, "connected");
        drainQueue();
    }

    private void onDisconnected(Slot slot, ConnectionRef connection) {
        Channel channel = connection.channel;
        synchronized (lock) {
            if (slot.connection != connection) return;
            slot.connection = null;
            readyConnections--;
            if (!closed) state = readyConnections == 0 ? RpcPoolState.RECONNECTING : RpcPoolState.DEGRADED;
        }
        failPending(slot, connection, new OutcomeUnknownException("RPC connection closed; request outcome is unknown"));
        LOG.warn("RPC pool disconnected connection={} remote={}",
                channel.id().asShortText(), channel.remoteAddress());
        emit(channel, "disconnected");
        scheduleConnect(0);
    }

    private void failPending(Slot slot, ConnectionRef connection, Throwable error) {
        for (Call<?> call : connection.pending.values()) {
            if (connection.pending.remove(call.requestId, call)) {
                release(slot);
                call.cancelResponseTimeout();
                finish(call, error);
            }
        }
    }

    private void emit(Channel channel, String reason) {
        if (listeners.isEmpty()) return;
        RpcConnectionEvent event = new RpcConnectionEvent(state,
                channel == null ? null : channel.id().asShortText(),
                channel == null ? null : String.valueOf(channel.localAddress()),
                channel == null ? null : String.valueOf(channel.remoteAddress()), reason);
        for (Consumer<RpcConnectionEvent> listener : listeners) {
            complete(() -> {
                try { listener.accept(event); }
                catch (RuntimeException e) { LOG.warn("RPC connection listener failed", e); }
            });
        }
    }

    @Override
    public void close() {
        List<Call<?>> waiting;
        List<CompletableFuture<Void>> waiters;
        synchronized (lock) {
            if (closed) return;
            closed = true;
            state = RpcPoolState.CLOSED;
            if (connectTask != null) connectTask.cancel(false);
            waiting = new ArrayList<>(queued);
            queued.clear();
            queuedCount = 0;
            queuedBytes = 0;
            waiters = new ArrayList<>(readyWaiters);
            readyWaiters.clear();
        }
        for (Call<?> call : waiting) {
            call.cancelWaitTimeout();
            finish(call, new RpcException("RPC pool is closed", null));
        }
        for (CompletableFuture<Void> waiter : waiters) {
            complete(() -> waiter.completeExceptionally(new RpcException("RPC pool is closed", null)));
        }
        for (Slot slot : slots) {
            ConnectionRef connection = slot.connection;
            if (connection != null) {
                connection.channel.close();
                failPending(slot, connection, new OutcomeUnknownException(
                        "RPC pool closed; request outcome is unknown"));
            }
        }
        boolean inEventLoop = false;
        for (EventExecutor executor : group) {
            if (executor.inEventLoop()) {
                inEventLoop = true;
                break;
            }
        }
        if (inEventLoop) group.shutdownGracefully(0, 2, TimeUnit.SECONDS);
        else group.shutdownGracefully(0, 2, TimeUnit.SECONDS).syncUninterruptibly();
        emit(null, "closed");
    }

    private final class PoolHandler extends SimpleChannelInboundHandler<NsHeadFrame> {
        private final Slot slot;
        private final ConnectionRef connection;
        private long heartbeatRequestId;
        private ScheduledFuture<?> heartbeatTimeout;

        private PoolHandler(Slot slot, ConnectionRef connection) {
            this.slot = slot;
            this.connection = connection;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            onConnected(slot, connection);
            ctx.fireChannelActive();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            cancelHeartbeat();
            onDisconnected(slot, connection);
            ctx.fireChannelInactive();
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
            if (event instanceof IdleStateEvent idle && idle.state() == IdleState.READER_IDLE) {
                sendHeartbeat(ctx);
                return;
            }
            ctx.fireUserEventTriggered(event);
        }

        private void sendHeartbeat(ChannelHandlerContext ctx) {
            if (heartbeatRequestId != 0 || !ctx.channel().isActive()) return;
            long id = nextId();
            heartbeatRequestId = id;
            heartbeatTimeout = ctx.executor().schedule(() -> {
                if (heartbeatRequestId == id) {
                    heartbeatTimeouts.incrementAndGet();
                    LOG.warn("RPC heartbeat timed out connection={}", ctx.channel().id().asShortText());
                    ctx.close();
                }
            }, options.heartbeatTimeoutMillis, TimeUnit.MILLISECONDS);
            ctx.writeAndFlush(new NsHeadFrame((int) id & 0xffff, NsHeadFrame.PROTOCOL_VERSION,
                    0, provider, NsHeadFrame.PING_METHOD_ID, id, new byte[0]))
                    .addListener(write -> { if (!write.isSuccess()) ctx.close(); });
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, NsHeadFrame frame) {
            if (frame.methodId() == NsHeadFrame.PONG_METHOD_ID) {
                if (frame.body().length != 0 || heartbeatRequestId == 0
                        || frame.requestId() != heartbeatRequestId) {
                    ctx.close();
                    return;
                }
                cancelHeartbeat();
                return;
            }
            if (frame.methodId() == NsHeadFrame.PING_METHOD_ID) { ctx.close(); return; }
            if (slot.connection != connection) return;
            Call<?> call = connection.pending.remove(frame.requestId());
            if (call == null) { discardedLateResponses.incrementAndGet(); return; }
            release(slot);
            call.cancelResponseTimeout();
            if (frame.methodId() != call.methodId || frame.logId() != call.logId) {
                finish(call, new RpcException("RPC response header does not match request", null));
                return;
            }
            try {
                RpcResponse envelope = RpcResponse.parseFrom(frame.body());
                if (envelope.getErrorCode() != 0) {
                    finish(call, new RpcException(Integer.toUnsignedLong(envelope.getErrorCode()),
                            envelope.getErrorMessage()));
                } else {
                    call.complete(envelope.getPayload().toByteArray());
                }
            } catch (Exception e) {
                finish(call, new RpcException("Invalid RPC response", e));
            }
        }

        private void cancelHeartbeat() {
            heartbeatRequestId = 0;
            if (heartbeatTimeout != null) heartbeatTimeout.cancel(false);
            heartbeatTimeout = null;
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            LOG.warn("RPC pool connection failed remote={}", ctx.channel().remoteAddress(), cause);
            ctx.close();
        }
    }

    private final class Slot {
        private volatile ConnectionRef connection;
        private final Semaphore permits;
        private final AtomicInteger inFlight = new AtomicInteger();

        private Slot(int maxInFlight) { permits = new Semaphore(maxInFlight); }
    }

    private final class ConnectionRef {
        private final Channel channel;
        private final ConcurrentMap<Long, Call<?>> pending = new ConcurrentHashMap<>();

        private ConnectionRef(Channel channel) { this.channel = channel; }
    }

    private record Selection(Slot slot, ConnectionRef connection) {}

    private final class Call<Resp extends Message> {
        private final long methodId;
        private final long logId;
        private final Parser<Resp> parser;
        private final byte[] body;
        private final CompletableFuture<Resp> future = new CompletableFuture<>();
        private volatile long requestId;
        private volatile Slot slot;
        private volatile ConnectionRef connection;
        private volatile ScheduledFuture<?> waitTimeout;
        private volatile ScheduledFuture<?> responseTimeout;

        private Call(long methodId, long logId, Parser<Resp> parser, byte[] body) {
            this.methodId = methodId;
            this.logId = logId;
            this.parser = parser;
            this.body = body;
        }

        private void cancelWaitTimeout() {
            ScheduledFuture<?> current = waitTimeout;
            if (current != null) current.cancel(false);
        }

        private void cancelResponseTimeout() {
            ScheduledFuture<?> current = responseTimeout;
            if (current != null) current.cancel(false);
        }

        private void complete(byte[] payload) throws Exception {
            finish(this, parser.parseFrom(payload));
        }
    }

    public static final class Builder {
        private final InetSocketAddress address;
        private final String provider;
        private int connections = 1;
        private int ioThreads;
        private int maxBodyLength = 4 * 1024 * 1024;
        private int maxInFlightPerConnection = 4_096;
        private int maxQueuedCalls = 1_024;
        private long maxQueuedBytes = 16L * 1024 * 1024;
        private long responseTimeoutMillis = 3_000;
        private long connectTimeoutMillis = 3_000;
        private long connectionWaitTimeoutMillis = 15_000;
        private long heartbeatIdleMillis = 30_000;
        private long heartbeatTimeoutMillis = 10_000;
        private long reconnectInitialMillis = 500;
        private long reconnectMaxMillis = 30_000;
        private int maxReconnectAttempts = 3;
        private Executor completionExecutor = ForkJoinPool.commonPool();

        private Builder(InetSocketAddress address, String provider) {
            this.address = Objects.requireNonNull(address, "address");
            this.provider = Objects.requireNonNull(provider, "provider");
            if (provider.getBytes(StandardCharsets.UTF_8).length > 16) {
                throw new IllegalArgumentException("provider must be at most 16 UTF-8 bytes");
            }
        }

        private Builder copy() {
            Builder result = new Builder(address, provider);
            result.connections = connections;
            result.ioThreads = ioThreads;
            result.maxBodyLength = maxBodyLength;
            result.maxInFlightPerConnection = maxInFlightPerConnection;
            result.maxQueuedCalls = maxQueuedCalls;
            result.maxQueuedBytes = maxQueuedBytes;
            result.responseTimeoutMillis = responseTimeoutMillis;
            result.connectTimeoutMillis = connectTimeoutMillis;
            result.connectionWaitTimeoutMillis = connectionWaitTimeoutMillis;
            result.heartbeatIdleMillis = heartbeatIdleMillis;
            result.heartbeatTimeoutMillis = heartbeatTimeoutMillis;
            result.reconnectInitialMillis = reconnectInitialMillis;
            result.reconnectMaxMillis = reconnectMaxMillis;
            result.maxReconnectAttempts = maxReconnectAttempts;
            result.completionExecutor = completionExecutor;
            return result;
        }

        private static long millis(Duration duration, String name) {
            Objects.requireNonNull(duration, name);
            long value = duration.toMillis();
            if (value < 1) throw new IllegalArgumentException(name + " must be positive");
            return value;
        }

        public Builder connections(int value) {
            if (value < 1) throw new IllegalArgumentException("connections must be positive");
            connections = value;
            return this;
        }

        public Builder ioThreads(int value) {
            if (value < 1) throw new IllegalArgumentException("ioThreads must be positive");
            ioThreads = value;
            return this;
        }

        private int resolvedIoThreads() {
            return ioThreads > 0 ? ioThreads : Math.min(connections, Runtime.getRuntime().availableProcessors());
        }

        public Builder maxBodyLength(int value) {
            if (value < 1) throw new IllegalArgumentException("maxBodyLength must be positive");
            maxBodyLength = value;
            return this;
        }

        public Builder maxInFlightPerConnection(int value) {
            if (value < 1) throw new IllegalArgumentException("maxInFlightPerConnection must be positive");
            maxInFlightPerConnection = value;
            return this;
        }

        public Builder maxQueuedCalls(int value) {
            if (value < 0) throw new IllegalArgumentException("maxQueuedCalls must be non-negative");
            maxQueuedCalls = value;
            return this;
        }

        public Builder maxQueuedBytes(long value) {
            if (value < 0) throw new IllegalArgumentException("maxQueuedBytes must be non-negative");
            maxQueuedBytes = value;
            return this;
        }

        public Builder responseTimeout(Duration value) {
            responseTimeoutMillis = millis(value, "responseTimeout");
            return this;
        }

        public Builder connectTimeout(Duration value) {
            long millis = millis(value, "connectTimeout");
            if (millis > Integer.MAX_VALUE) throw new IllegalArgumentException("connectTimeout is too long");
            connectTimeoutMillis = millis;
            return this;
        }

        public Builder connectionWaitTimeout(Duration value) {
            connectionWaitTimeoutMillis = millis(value, "connectionWaitTimeout");
            return this;
        }

        public Builder heartbeatIdle(Duration value) {
            heartbeatIdleMillis = millis(value, "heartbeatIdle");
            return this;
        }

        public Builder heartbeatTimeout(Duration value) {
            heartbeatTimeoutMillis = millis(value, "heartbeatTimeout");
            return this;
        }

        public Builder reconnectBackoff(Duration initial, Duration maximum) {
            long start = millis(initial, "reconnectInitial");
            long end = millis(maximum, "reconnectMaximum");
            if (end < start) throw new IllegalArgumentException("reconnect maximum must be >= initial");
            reconnectInitialMillis = start;
            reconnectMaxMillis = end;
            return this;
        }

        public Builder maxReconnectAttempts(int value) {
            if (value < 1) throw new IllegalArgumentException("maxReconnectAttempts must be positive");
            maxReconnectAttempts = value;
            return this;
        }

        public Builder completionExecutor(Executor value) {
            completionExecutor = Objects.requireNonNull(value, "completionExecutor");
            return this;
        }

        public RpcClientPool build() {
            return new RpcClientPool(this);
        }
    }
}
