package dev.yumix.rpc.client;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import dev.yumix.rpc.protocol.NsHeadFrame;
import dev.yumix.rpc.protocol.NsHeadFrameDecoder;
import dev.yumix.rpc.protocol.NsHeadFrameEncoder;
import dev.yumix.rpc.protocol.RpcResponse;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
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
import io.netty.util.concurrent.ScheduledFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** Multiplexed RPC client with heartbeat detection and automatic reconnection. */
public final class RpcClient implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(RpcClient.class);

    private final InetSocketAddress address;
    private final String provider;
    private final RpcClientConfig config;
    private final EventLoopGroup group;
    private final Bootstrap bootstrap;
    private final ConcurrentMap<Long, Pending<?>> pending = new ConcurrentHashMap<>();
    private final AtomicLong nextRequestId = new AtomicLong(ThreadLocalRandom.current().nextLong());
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean connecting = new AtomicBoolean();
    private final AtomicBoolean reconnectScheduled = new AtomicBoolean();
    private final AtomicInteger reconnectAttempts = new AtomicInteger();
    private volatile Channel channel;
    private volatile ScheduledFuture<?> reconnectTask;

    public static RpcClient connect(InetSocketAddress address, String provider,
                                    long timeoutMillis, int maxBodyLength) {
        return connect(address, provider, RpcClientConfig.defaults(timeoutMillis, maxBodyLength));
    }

    public static RpcClient connect(InetSocketAddress address, String provider, RpcClientConfig config) {
        RpcClient client = new RpcClient(address, provider, config);
        try {
            ChannelFuture future = client.bootstrap.connect(address).syncUninterruptibly();
            if (!future.isSuccess()) throw new RpcException("Initial RPC connection failed", future.cause());
            client.onConnected(future.channel());
            return client;
        } catch (RuntimeException e) {
            client.close();
            throw e;
        }
    }

    public static RpcClient open(InetSocketAddress address, String provider, RpcClientConfig config) {
        RpcClient client = new RpcClient(address, provider, config);
        client.reconnect();
        return client;
    }

    public static RpcClient open(InetSocketAddress address, String provider,
                                 long timeoutMillis, int maxBodyLength) {
        return open(address, provider, RpcClientConfig.defaults(timeoutMillis, maxBodyLength));
    }

    private RpcClient(InetSocketAddress address, String provider, RpcClientConfig config) {
        if (address == null || config == null) throw new IllegalArgumentException("address and config are required");
        if (provider == null || provider.getBytes(StandardCharsets.UTF_8).length > 16) {
            throw new IllegalArgumentException("provider must be non-null and at most 16 UTF-8 bytes");
        }
        this.address = address;
        this.provider = provider;
        this.config = config;
        this.group = new NioEventLoopGroup(1, new DefaultThreadFactory("rpc-client"));
        this.bootstrap = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.connectTimeoutMillis())
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel socket) {
                        socket.pipeline().addLast(new NsHeadFrameDecoder(config.maxBodyLength()));
                        socket.pipeline().addLast(new NsHeadFrameEncoder());
                        socket.pipeline().addLast(new IdleStateHandler(
                                config.heartbeatIdleMillis(), 0, 0, TimeUnit.MILLISECONDS));
                        socket.pipeline().addLast(new ClientHandler(RpcClient.this));
                    }
                });
    }

    public <Resp extends Message> CompletableFuture<Resp> request(
            long methodId, long logId, Message request, Parser<Resp> responseParser) {
        if (methodId <= NsHeadFrame.PING_METHOD_ID || methodId >= NsHeadFrame.PONG_METHOD_ID) {
            throw new IllegalArgumentException("methodId is reserved for heartbeat control frames");
        }
        if ((logId & ~0xffff_ffffL) != 0) throw new IllegalArgumentException("logId must fit in uint32");
        Channel current = channel;
        if (current == null || !current.isActive()) {
            return CompletableFuture.failedFuture(new RpcException("RPC connection is unavailable", null));
        }
        byte[] body = request.toByteArray();
        if (body.length > config.maxBodyLength()) throw new IllegalArgumentException("request body exceeds configured limit");

        Pending<Resp> call = registerPending(methodId, logId, responseParser);
        call.timeout = current.eventLoop().schedule(() -> {
            if (pending.remove(call.requestId, call)) {
                call.future.completeExceptionally(new TimeoutException(
                        "RPC timed out after " + config.requestTimeoutMillis() + " ms"));
            }
        }, config.requestTimeoutMillis(), TimeUnit.MILLISECONDS);
        NsHeadFrame frame = new NsHeadFrame((int) call.requestId & 0xffff,
                NsHeadFrame.PROTOCOL_VERSION, logId, provider, methodId, call.requestId, body);
        current.writeAndFlush(frame).addListener(write -> {
            if (!write.isSuccess() && pending.remove(call.requestId, call)) {
                call.cancelTimeout();
                call.future.completeExceptionally(new RpcException("Failed to write RPC request", write.cause()));
            }
        });
        return call.future;
    }

    private <Resp extends Message> Pending<Resp> registerPending(long methodId, long logId, Parser<Resp> parser) {
        while (true) {
            long requestId = nextId();
            Pending<Resp> call = new Pending<>(requestId, methodId, logId, parser);
            if (pending.putIfAbsent(requestId, call) == null) return call;
        }
    }

    private long nextId() {
        long id;
        do { id = nextRequestId.incrementAndGet(); } while (id == 0);
        return id;
    }

    public boolean isConnected() {
        Channel current = channel;
        return current != null && current.isActive();
    }

    public SocketAddress localAddress() {
        Channel current = channel;
        return current == null ? null : current.localAddress();
    }

    public SocketAddress remoteAddress() {
        Channel current = channel;
        return current == null ? null : current.remoteAddress();
    }

    private void onConnected(Channel connected) {
        if (closed.get()) {
            connected.close();
            return;
        }
        if (channel == connected) return;
        channel = connected;
        reconnectAttempts.set(0);
        LOG.info("RPC client connected local={} remote={} connection={}",
                connected.localAddress(), connected.remoteAddress(), connected.id().asShortText());
    }

    private void onDisconnected(Channel disconnected) {
        if (channel != disconnected) return;
        channel = null;
        failPending(new RpcException("RPC connection closed; request outcome is unknown", null));
        if (!closed.get()) {
            LOG.warn("RPC client disconnected remote={}", address);
            scheduleReconnect();
        }
    }

    private void scheduleReconnect() {
        if (closed.get() || !reconnectScheduled.compareAndSet(false, true)) return;
        int attempt = reconnectAttempts.getAndIncrement();
        long base = config.reconnectInitialMillis();
        for (int i = 0; i < attempt && base < config.reconnectMaxMillis(); i++) {
            base = base >= config.reconnectMaxMillis() / 2
                    ? config.reconnectMaxMillis() : base * 2;
        }
        long delay = Math.max(1, Math.min(config.reconnectMaxMillis(),
                (long) (base * ThreadLocalRandom.current().nextDouble(0.8, 1.2))));
        LOG.info("RPC reconnect scheduled remote={} attempt={} delayMs={}", address, attempt + 1, delay);
        reconnectTask = group.next().schedule(() -> {
            reconnectScheduled.set(false);
            reconnect();
        }, delay, TimeUnit.MILLISECONDS);
    }

    private void reconnect() {
        if (closed.get() || isConnected() || !connecting.compareAndSet(false, true)) return;
        bootstrap.connect(address).addListener((ChannelFutureListener) future -> {
            connecting.set(false);
            if (closed.get()) {
                if (future.isSuccess()) future.channel().close();
            } else if (!future.isSuccess()) {
                LOG.warn("RPC reconnect failed remote={} reason={}", address, future.cause().toString());
                scheduleReconnect();
            }
        });
    }

    private void failPending(Throwable error) {
        for (Pending<?> call : pending.values()) {
            if (pending.remove(call.requestId, call)) {
                call.cancelTimeout();
                call.future.completeExceptionally(error);
            }
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        ScheduledFuture<?> scheduled = reconnectTask;
        if (scheduled != null) scheduled.cancel(false);
        Channel current = channel;
        channel = null;
        if (current != null) {
            ChannelFuture future = current.close();
            if (!current.eventLoop().inEventLoop()) future.syncUninterruptibly();
        }
        failPending(new RpcException("RPC client closed", null));
        if (group.next().inEventLoop()) group.shutdownGracefully();
        else group.shutdownGracefully().syncUninterruptibly();
    }

    private static final class ClientHandler extends SimpleChannelInboundHandler<NsHeadFrame> {
        private final RpcClient client;
        private long heartbeatRequestId;
        private ScheduledFuture<?> heartbeatTimeout;

        private ClientHandler(RpcClient client) { this.client = client; }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            client.onConnected(ctx.channel());
            ctx.fireChannelActive();
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
            long requestId = client.nextId();
            heartbeatRequestId = requestId;
            heartbeatTimeout = ctx.executor().schedule(() -> {
                if (heartbeatRequestId == requestId) {
                    LOG.warn("RPC heartbeat timed out remote={} connection={}",
                            ctx.channel().remoteAddress(), ctx.channel().id().asShortText());
                    ctx.close();
                }
            }, client.config.heartbeatTimeoutMillis(), TimeUnit.MILLISECONDS);
            ctx.writeAndFlush(new NsHeadFrame((int) requestId & 0xffff,
                    NsHeadFrame.PROTOCOL_VERSION, 0, client.provider,
                    NsHeadFrame.PING_METHOD_ID, requestId, new byte[0])).addListener(write -> {
                if (!write.isSuccess()) ctx.close();
            });
            LOG.debug("RPC heartbeat ping remote={} requestId={}",
                    ctx.channel().remoteAddress(), Long.toUnsignedString(requestId));
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, NsHeadFrame frame) {
            if (frame.methodId() == NsHeadFrame.PING_METHOD_ID) {
                LOG.warn("Unexpected heartbeat ping from server remote={}", ctx.channel().remoteAddress());
                ctx.close();
                return;
            }
            if (frame.methodId() == NsHeadFrame.PONG_METHOD_ID) {
                if (frame.body().length != 0 ||
                        (heartbeatRequestId != 0 && frame.requestId() != heartbeatRequestId)) {
                    ctx.close();
                    return;
                }
                cancelHeartbeat();
                LOG.debug("RPC heartbeat pong remote={} requestId={}",
                        ctx.channel().remoteAddress(), Long.toUnsignedString(frame.requestId()));
                return;
            }
            cancelHeartbeat();
            Pending<?> call = client.pending.remove(frame.requestId());
            if (call == null) return;
            call.cancelTimeout();
            if (frame.methodId() != call.methodId || frame.logId() != call.logId) {
                call.future.completeExceptionally(new RpcException("RPC response header does not match request", null));
                return;
            }
            try {
                RpcResponse response = RpcResponse.parseFrom(frame.body());
                if (response.getErrorCode() != 0) {
                    call.future.completeExceptionally(new RpcException(
                            Integer.toUnsignedLong(response.getErrorCode()), response.getErrorMessage()));
                } else {
                    call.complete(response.getPayload().toByteArray());
                }
            } catch (Exception e) {
                call.future.completeExceptionally(new RpcException("Invalid RPC response", e));
            }
        }

        private void cancelHeartbeat() {
            heartbeatRequestId = 0;
            if (heartbeatTimeout != null) heartbeatTimeout.cancel(false);
            heartbeatTimeout = null;
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            cancelHeartbeat();
            client.onDisconnected(ctx.channel());
            ctx.fireChannelInactive();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            LOG.warn("RPC client connection failed remote={}", ctx.channel().remoteAddress(), cause);
            ctx.close();
        }
    }

    private static final class Pending<Resp extends Message> {
        private final long requestId;
        private final long methodId;
        private final long logId;
        private final Parser<Resp> parser;
        private final CompletableFuture<Resp> future = new CompletableFuture<>();
        private volatile ScheduledFuture<?> timeout;

        private Pending(long requestId, long methodId, long logId, Parser<Resp> parser) {
            this.requestId = requestId;
            this.methodId = methodId;
            this.logId = logId;
            this.parser = parser;
        }

        private void complete(byte[] payload) throws Exception {
            future.complete(parser.parseFrom(payload));
        }

        private void cancelTimeout() {
            ScheduledFuture<?> current = timeout;
            if (current != null) current.cancel(false);
        }
    }
}
