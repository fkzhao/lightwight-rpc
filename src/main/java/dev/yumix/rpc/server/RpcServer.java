package dev.yumix.rpc.server;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import dev.yumix.rpc.protocol.NsHeadFrame;
import dev.yumix.rpc.protocol.NsHeadFrameDecoder;
import dev.yumix.rpc.protocol.NsHeadFrameEncoder;
import dev.yumix.rpc.protocol.RpcRequest;
import dev.yumix.rpc.protocol.RpcResponse;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Netty based nshead + protobuf RPC server. Register methods before start(). */
public final class RpcServer implements AutoCloseable {
    private static final long ERROR_METHOD_NOT_FOUND = 1;
    private static final long ERROR_BAD_REQUEST = 2;
    private static final long ERROR_INTERNAL = 3;

    private final InetSocketAddress address;
    private final String provider;
    private final int maxBodyLength;
    private final EventLoopGroup boss = new NioEventLoopGroup(1, new DefaultThreadFactory("rpc-accept"));
    private final EventLoopGroup ioGroup = new NioEventLoopGroup(0, new DefaultThreadFactory("rpc-io"));
    private final ExecutorService workers;
    private final Map<Long, RegisteredMethod<?, ?>> methods = new ConcurrentHashMap<>();
    private volatile Channel serverChannel;

    public RpcServer(InetSocketAddress address, String provider, int workerThreads, int maxBodyLength) {
        this.address = Objects.requireNonNull(address, "address");
        this.provider = Objects.requireNonNull(provider, "provider");
        if (workerThreads < 1) throw new IllegalArgumentException("workerThreads must be positive");
        if (maxBodyLength < 0) throw new IllegalArgumentException("maxBodyLength must be non-negative");
        if (provider.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 16) {
            throw new IllegalArgumentException("provider must be at most 16 UTF-8 bytes");
        }
        this.maxBodyLength = maxBodyLength;
        this.workers = Executors.newFixedThreadPool(workerThreads, new DefaultThreadFactory("rpc-worker"));
    }

    public <Req extends Message, Resp extends Message> RpcServer register(
            long methodId, Parser<Req> requestParser, RpcMethod<Req, Resp> method) {
        if ((methodId & ~0xffff_ffffL) != 0) throw new IllegalArgumentException("methodId must fit in uint32");
        RegisteredMethod<Req, Resp> registered = new RegisteredMethod<>(requestParser, method);
        if (methods.putIfAbsent(methodId, registered) != null) {
            throw new IllegalArgumentException("methodId already registered: " + methodId);
        }
        return this;
    }

    public RpcServer start() {
        if (serverChannel != null) throw new IllegalStateException("server already started");
        ServerBootstrap bootstrap = new ServerBootstrap()
                .group(boss, ioGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel channel) {
                        channel.pipeline().addLast(new NsHeadFrameDecoder(maxBodyLength));
                        channel.pipeline().addLast(new NsHeadFrameEncoder());
                        channel.pipeline().addLast(new RequestHandler());
                    }
                });
        serverChannel = bootstrap.bind(address).syncUninterruptibly().channel();
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
        Channel current = serverChannel;
        if (current != null) current.close().syncUninterruptibly();
        workers.shutdownNow();
        boss.shutdownGracefully().syncUninterruptibly();
        ioGroup.shutdownGracefully().syncUninterruptibly();
    }

    private final class RequestHandler extends io.netty.channel.SimpleChannelInboundHandler<NsHeadFrame> {
        @Override
        protected void channelRead0(io.netty.channel.ChannelHandlerContext ctx, NsHeadFrame header) {
            if (header.version() != NsHeadFrame.PROTOCOL_VERSION) {
                ctx.close();
                return;
            }
            RpcRequest request;
            try {
                request = RpcRequest.parseFrom(header.body());
                if (request.getRequestId() == 0 || header.id() != ((int) request.getRequestId() & 0xffff)) {
                    ctx.close();
                    return;
                }
            } catch (Exception e) {
                ctx.close();
                return;
            }
            RegisteredMethod<?, ?> method = methods.get(header.methodId());
            if (method == null) {
                respond(ctx, header, request.getRequestId(), null,
                        ERROR_METHOD_NOT_FOUND, "Unknown methodId: " + header.methodId());
                return;
            }
            try {
                workers.execute(() -> {
                    try {
                        Message response = method.invoke(request.getPayload().toByteArray());
                        respond(ctx, header, request.getRequestId(), response, 0, "");
                    } catch (Exception e) {
                        respond(ctx, header, request.getRequestId(), null,
                                ERROR_BAD_REQUEST, "RPC method failed: " + safeMessage(e));
                    }
                });
            } catch (RuntimeException e) {
                respond(ctx, header, request.getRequestId(), null, ERROR_INTERNAL, "RPC worker unavailable");
            }
        }

        private void respond(io.netty.channel.ChannelHandlerContext ctx, NsHeadFrame request, long requestId,
                             Message message, long code, String error) {
            RpcResponse.Builder response = RpcResponse.newBuilder().setRequestId(requestId)
                    .setErrorCode((int) code).setErrorMessage(error);
            if (message != null) response.setPayload(message.toByteString());
            byte[] body = response.build().toByteArray();
            if (body.length > maxBodyLength) {
                body = RpcResponse.newBuilder().setRequestId(requestId).setErrorCode((int) ERROR_INTERNAL)
                        .setErrorMessage("Response exceeds configured body limit").build().toByteArray();
            }
            ctx.writeAndFlush(new NsHeadFrame(request.id(), NsHeadFrame.PROTOCOL_VERSION,
                    request.logId(), provider, request.methodId(), body));
        }

        @Override
        public void exceptionCaught(io.netty.channel.ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
        }
    }

    private static String safeMessage(Exception e) {
        String message = e.getMessage();
        return message == null || message.isBlank() ? e.getClass().getSimpleName() : message;
    }

    private record RegisteredMethod<Req extends Message, Resp extends Message>(
            Parser<Req> parser, RpcMethod<Req, Resp> method) {
        private Message invoke(byte[] body) throws Exception {
            Req request = parser.parseFrom(body);
            return method.handle(request);
        }
    }
}
