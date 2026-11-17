package dev.yumix.rpc.server;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import dev.yumix.rpc.RpcMethodDescriptor;
import dev.yumix.rpc.protocol.NsHeadFrame;
import dev.yumix.rpc.protocol.NsHeadFrameDecoder;
import dev.yumix.rpc.protocol.NsHeadFrameEncoder;
import dev.yumix.rpc.protocol.RpcResponse;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Netty based nshead + protobuf RPC server. Register methods before start(). */
public final class RpcServer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(RpcServer.class);
    private static final long ERROR_METHOD_NOT_FOUND = 1;
    private static final long ERROR_BAD_REQUEST = 2;
    private static final long ERROR_INTERNAL = 3;
    private static final long ERROR_OVERLOADED = 4;

    private final InetSocketAddress address;
    private final String provider;
    private final int maxBodyLength;
    private final long readIdleMillis;
    private final boolean logRequests;
    private final EventLoopGroup boss = new NioEventLoopGroup(1, new DefaultThreadFactory("rpc-accept"));
    private final EventLoopGroup ioGroup = new NioEventLoopGroup(0, new DefaultThreadFactory("rpc-io"));
    private final ThreadPoolExecutor workers;
    private final ChannelGroup connections = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Map<Long, RegisteredMethod<?, ?>> methods = new ConcurrentHashMap<>();
    private volatile Channel serverChannel;

    public RpcServer(InetSocketAddress address, String provider, int workerThreads, int maxBodyLength) {
        this(address, provider, workerThreads, maxBodyLength, 90_000);
    }

    public RpcServer(InetSocketAddress address, String provider, int workerThreads,
                     int maxBodyLength, long readIdleMillis) {
        this(address, provider, workerThreads, maxBodyLength, readIdleMillis, true);
    }

    public RpcServer(InetSocketAddress address, String provider, int workerThreads,
                     int maxBodyLength, long readIdleMillis, boolean logRequests) {
        this(address, provider, workerThreads, maxBodyLength, readIdleMillis, logRequests, 4_096);
    }

    public RpcServer(InetSocketAddress address, String provider, int workerThreads,
                     int maxBodyLength, long readIdleMillis, boolean logRequests, int workerQueueCapacity) {
        this.address = Objects.requireNonNull(address, "address");
        this.provider = Objects.requireNonNull(provider, "provider");
        if (workerThreads < 1) throw new IllegalArgumentException("workerThreads must be positive");
        if (maxBodyLength < 0) throw new IllegalArgumentException("maxBodyLength must be non-negative");
        if (readIdleMillis < 1) throw new IllegalArgumentException("readIdleMillis must be positive");
        if (workerQueueCapacity < 1) throw new IllegalArgumentException("workerQueueCapacity must be positive");
        if (provider.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 16) {
            throw new IllegalArgumentException("provider must be at most 16 UTF-8 bytes");
        }
        this.maxBodyLength = maxBodyLength;
        this.readIdleMillis = readIdleMillis;
        this.logRequests = logRequests;
        this.workers = new ThreadPoolExecutor(workerThreads, workerThreads, 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(workerQueueCapacity), new DefaultThreadFactory("rpc-worker"),
                new ThreadPoolExecutor.AbortPolicy());
    }

    public <Req extends Message, Resp extends Message> RpcServer register(
            long methodId, Parser<Req> requestParser, RpcMethod<Req, Resp> method) {
        Objects.requireNonNull(method, "method");
        return registerWithContext(methodId, requestParser, (request, context) -> method.handle(request));
    }

    public <Req extends Message, Resp extends Message> RpcServer register(
            RpcMethodDescriptor<Req, Resp> descriptor, RpcMethod<Req, Resp> method) {
        return register(descriptor.methodId(), descriptor.requestParser(), method);
    }

    public <Req extends Message, Resp extends Message> RpcServer registerWithContext(
            RpcMethodDescriptor<Req, Resp> descriptor, RpcMethodWithContext<Req, Resp> method) {
        return registerWithContext(descriptor.methodId(), descriptor.requestParser(), method);
    }

    public <Req extends Message, Resp extends Message> RpcServer registerWithContext(
            long methodId, Parser<Req> requestParser, RpcMethodWithContext<Req, Resp> method) {
        if (methodId <= NsHeadFrame.PING_METHOD_ID || methodId >= NsHeadFrame.PONG_METHOD_ID) {
            throw new IllegalArgumentException("methodId is reserved for heartbeat control frames");
        }
        RegisteredMethod<Req, Resp> registered = new RegisteredMethod<>(
                Objects.requireNonNull(requestParser, "requestParser"), Objects.requireNonNull(method, "method"));
        if (methods.putIfAbsent(methodId, registered) != null) {
            throw new IllegalArgumentException("methodId already registered: " + methodId);
        }
        return this;
    }

    public RpcServer start() {
        if (closed.get()) throw new IllegalStateException("server is closed");
        if (serverChannel != null) throw new IllegalStateException("server already started");
        ServerBootstrap bootstrap = new ServerBootstrap()
                .group(boss, ioGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel channel) {
                        connections.add(channel);
                        channel.pipeline().addLast(new NsHeadFrameDecoder(maxBodyLength));
                        channel.pipeline().addLast(new NsHeadFrameEncoder());
                        channel.pipeline().addLast(new IdleStateHandler(readIdleMillis, 0, 0, TimeUnit.MILLISECONDS));
                        channel.pipeline().addLast(new RequestHandler());
                    }
                });
        serverChannel = bootstrap.bind(address).syncUninterruptibly().channel();
        LOG.info("RPC server listening on {}", serverChannel.localAddress());
        return this;
    }

    public void awaitTermination() throws InterruptedException {
        Channel current = serverChannel;
        if (current == null) throw new IllegalStateException("server is not started");
        current.closeFuture().sync();
    }

    public InetSocketAddress localAddress() {
        Channel current = serverChannel;
        if (current == null) throw new IllegalStateException("server is not started");
        return (InetSocketAddress) current.localAddress();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        Channel current = serverChannel;
        if (current != null) current.close().syncUninterruptibly();
        connections.close().awaitUninterruptibly();
        workers.shutdownNow();
        boss.shutdownGracefully().syncUninterruptibly();
        ioGroup.shutdownGracefully().syncUninterruptibly();
    }

    private final class RequestHandler extends io.netty.channel.SimpleChannelInboundHandler<NsHeadFrame> {
        @Override
        public void userEventTriggered(io.netty.channel.ChannelHandlerContext ctx, Object event) {
            if (event instanceof IdleStateEvent idle && idle.state() == IdleState.READER_IDLE) {
                LOG.info("RPC connection idle; closing connection={} remote={}",
                        ctx.channel().id().asShortText(), ctx.channel().remoteAddress());
                ctx.close();
                return;
            }
            ctx.fireUserEventTriggered(event);
        }

        @Override
        public void channelActive(io.netty.channel.ChannelHandlerContext ctx) {
            LOG.info("RPC connection opened connection={} local={} remote={}",
                    ctx.channel().id().asShortText(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
            ctx.fireChannelActive();
        }

        @Override
        public void channelInactive(io.netty.channel.ChannelHandlerContext ctx) {
            LOG.info("RPC connection closed connection={} remote={}",
                    ctx.channel().id().asShortText(), ctx.channel().remoteAddress());
            ctx.fireChannelInactive();
        }

        @Override
        protected void channelRead0(io.netty.channel.ChannelHandlerContext ctx, NsHeadFrame header) {
            if (header.methodId() == NsHeadFrame.PING_METHOD_ID) {
                if (header.body().length != 0) {
                    LOG.warn("Invalid heartbeat body connection={}", ctx.channel().id().asShortText());
                    ctx.close();
                    return;
                }
                ctx.writeAndFlush(new NsHeadFrame(header.id(), NsHeadFrame.PROTOCOL_VERSION,
                        header.logId(), provider, NsHeadFrame.PONG_METHOD_ID, header.requestId(), new byte[0]));
                LOG.debug("RPC heartbeat pong connection={} requestId={}",
                        ctx.channel().id().asShortText(), Long.toUnsignedString(header.requestId()));
                return;
            }
            if (header.methodId() == NsHeadFrame.PONG_METHOD_ID) {
                LOG.warn("Unexpected heartbeat pong from client connection={}", ctx.channel().id().asShortText());
                ctx.close();
                return;
            }
            long startedAt = System.nanoTime();
            String requestId = Long.toUnsignedString(header.requestId());
            String connectionId = ctx.channel().id().asShortText();
            if (logRequests) {
                LOG.info("RPC request received connection={} requestId={} methodId={} logId={} remote={}",
                        connectionId, requestId, header.methodId(), header.logId(), ctx.channel().remoteAddress());
            }
            RegisteredMethod<?, ?> method = methods.get(header.methodId());
            if (method == null) {
                LOG.warn("RPC unknown method connection={} requestId={} methodId={}",
                        connectionId, requestId, header.methodId());
                respond(ctx, header, null,
                        ERROR_METHOD_NOT_FOUND, "Unknown methodId: " + header.methodId());
                return;
            }
            RpcContext rpcContext = new RpcContext(header.requestId(), ctx.channel().id().asShortText(),
                    ctx.channel().localAddress(), ctx.channel().remoteAddress());
            try {
                workers.execute(() -> {
                    try {
                        Message response = method.invoke(header.body(), rpcContext);
                        respond(ctx, header, response, 0, "");
                        if (logRequests) {
                            LOG.info("RPC request completed connection={} requestId={} methodId={} elapsedMs={}",
                                    connectionId, requestId, header.methodId(), elapsedMillis(startedAt));
                        }
                    } catch (Exception e) {
                        LOG.warn("RPC request failed connection={} requestId={} methodId={} elapsedMs={}",
                                connectionId, requestId, header.methodId(), elapsedMillis(startedAt), e);
                        respond(ctx, header, null,
                                ERROR_BAD_REQUEST, "RPC method failed: " + safeMessage(e));
                    }
                });
            } catch (RejectedExecutionException e) {
                LOG.warn("RPC worker queue full connection={} requestId={}", connectionId, requestId);
                respond(ctx, header, null, ERROR_OVERLOADED, "RPC server overloaded");
            }
        }

        private void respond(io.netty.channel.ChannelHandlerContext ctx, NsHeadFrame request,
                             Message message, long code, String error) {
            RpcResponse.Builder response = RpcResponse.newBuilder()
                    .setErrorCode((int) code).setErrorMessage(error);
            if (message != null) response.setPayload(message.toByteString());
            byte[] body = response.build().toByteArray();
            if (body.length > maxBodyLength) {
                body = RpcResponse.newBuilder().setErrorCode((int) ERROR_INTERNAL)
                        .setErrorMessage("Response exceeds configured body limit").build().toByteArray();
            }
            ctx.writeAndFlush(new NsHeadFrame(request.id(), NsHeadFrame.PROTOCOL_VERSION,
                    request.logId(), provider, request.methodId(), request.requestId(), body));
        }

        @Override
        public void exceptionCaught(io.netty.channel.ChannelHandlerContext ctx, Throwable cause) {
            LOG.warn("RPC connection failed connection={} remote={}",
                    ctx.channel().id().asShortText(), ctx.channel().remoteAddress(), cause);
            ctx.close();
        }
    }

    private static long elapsedMillis(long startedAt) {
        return java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);
    }

    private static String safeMessage(Exception e) {
        String message = e.getMessage();
        return message == null || message.isBlank() ? e.getClass().getSimpleName() : message;
    }

    private record RegisteredMethod<Req extends Message, Resp extends Message>(
            Parser<Req> parser, RpcMethodWithContext<Req, Resp> method) {
        private Message invoke(byte[] body, RpcContext context) throws Exception {
            Req request = parser.parseFrom(body);
            return method.handle(request, context);
        }
    }
}
