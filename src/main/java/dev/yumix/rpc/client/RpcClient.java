package dev.yumix.rpc.client;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import dev.yumix.rpc.protocol.NsHeadFrame;
import dev.yumix.rpc.protocol.NsHeadFrameDecoder;
import dev.yumix.rpc.protocol.NsHeadFrameEncoder;
import dev.yumix.rpc.protocol.RpcRequest;
import dev.yumix.rpc.protocol.RpcResponse;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/** Multiplexed asynchronous client backed by a single Netty TCP connection. */
public final class RpcClient implements AutoCloseable {
    private final String provider;
    private final long timeoutMillis;
    private final int maxBodyLength;
    private final EventLoopGroup group;
    private final ConcurrentMap<Long, Pending<?>> pending = new ConcurrentHashMap<>();
    private final AtomicLong nextRequestId = new AtomicLong(ThreadLocalRandom.current().nextLong());
    private final Channel channel;

    public static RpcClient connect(InetSocketAddress address, String provider,
                                    long timeoutMillis, int maxBodyLength) {
        if (timeoutMillis < 1) throw new IllegalArgumentException("timeoutMillis must be positive");
        if (maxBodyLength < 0) throw new IllegalArgumentException("maxBodyLength must be non-negative");
        if (provider == null || provider.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 16) {
            throw new IllegalArgumentException("provider must be non-null and at most 16 UTF-8 bytes");
        }
        EventLoopGroup group = new NioEventLoopGroup(1, new DefaultThreadFactory("rpc-client"));
        try {
            ClientHandler handler = new ClientHandler();
            Channel channel = new Bootstrap()
                    .group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel socket) {
                            socket.pipeline().addLast(new NsHeadFrameDecoder(maxBodyLength));
                            socket.pipeline().addLast(new NsHeadFrameEncoder());
                            socket.pipeline().addLast(handler);
                        }
                    })
                    .connect(address).syncUninterruptibly().channel();
            return new RpcClient(provider, timeoutMillis, maxBodyLength, group, channel, handler);
        } catch (RuntimeException e) {
            group.shutdownGracefully();
            throw e;
        }
    }

    private RpcClient(String provider, long timeoutMillis, int maxBodyLength,
                      EventLoopGroup group, Channel channel, ClientHandler handler) {
        this.provider = provider;
        this.timeoutMillis = timeoutMillis;
        this.maxBodyLength = maxBodyLength;
        this.group = group;
        this.channel = channel;
        handler.client = this;
    }

    public <Resp extends Message> CompletableFuture<Resp> request(
            long methodId, long logId, Message request, Parser<Resp> responseParser) {
        if ((methodId & ~0xffff_ffffL) != 0) throw new IllegalArgumentException("methodId must fit in uint32");
        if ((logId & ~0xffff_ffffL) != 0) throw new IllegalArgumentException("logId must fit in uint32");
        if (!channel.isActive()) return CompletableFuture.failedFuture(new RpcException("RPC channel is closed", null));

        Pending<Resp> call = registerPending(methodId, logId, responseParser);
        byte[] body = RpcRequest.newBuilder().setRequestId(call.requestId)
                .setPayload(request.toByteString()).build().toByteArray();
        if (body.length > maxBodyLength) {
            pending.remove(call.requestId, call);
            throw new IllegalArgumentException("request body exceeds configured limit");
        }
        call.timeout = channel.eventLoop().schedule(() -> {
            if (pending.remove(call.requestId, call)) {
                call.future.completeExceptionally(new TimeoutException("RPC timed out after " + timeoutMillis + " ms"));
            }
        }, timeoutMillis, TimeUnit.MILLISECONDS);
        NsHeadFrame frame = new NsHeadFrame((int) call.requestId & 0xffff,
                NsHeadFrame.PROTOCOL_VERSION, logId, provider, methodId, body);
        channel.writeAndFlush(frame).addListener(write -> {
            if (!write.isSuccess() && pending.remove(call.requestId, call)) {
                call.cancelTimeout();
                call.future.completeExceptionally(new RpcException("Failed to write RPC request", write.cause()));
            }
        });
        return call.future;
    }

    private <Resp extends Message> Pending<Resp> registerPending(long methodId, long logId, Parser<Resp> parser) {
        while (true) {
            long requestId = nextRequestId.incrementAndGet();
            if (requestId == 0) continue;
            Pending<Resp> call = new Pending<>(requestId, methodId, logId, parser);
            if (pending.putIfAbsent(requestId, call) == null) return call;
        }
    }

    public SocketAddress localAddress() { return channel.localAddress(); }

    public SocketAddress remoteAddress() { return channel.remoteAddress(); }

    @Override
    public void close() {
        channel.close().syncUninterruptibly();
        for (Pending<?> call : pending.values()) {
            if (pending.remove(call.requestId, call)) {
                call.cancelTimeout();
                call.future.completeExceptionally(new RpcException("RPC client closed", null));
            }
        }
        group.shutdownGracefully().syncUninterruptibly();
    }

    private static final class ClientHandler extends SimpleChannelInboundHandler<NsHeadFrame> {
        private volatile RpcClient client;

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, NsHeadFrame frame) {
            if (frame.version() != NsHeadFrame.PROTOCOL_VERSION) {
                failPending(new RpcException("Unsupported RPC protocol version: " + frame.version(), null));
                ctx.close();
                return;
            }
            RpcResponse response;
            try {
                response = RpcResponse.parseFrom(frame.body());
            } catch (Exception e) {
                failPending(new RpcException("Invalid RPC response", e));
                ctx.close();
                return;
            }
            if (response.getRequestId() == 0) {
                failPending(new RpcException("RPC response has no request ID", null));
                ctx.close();
                return;
            }
            Pending<?> call = client.pending.remove(response.getRequestId());
            if (call == null) return;
            call.cancelTimeout();
            if (frame.id() != ((int) call.requestId & 0xffff)
                    || frame.methodId() != call.methodId || frame.logId() != call.logId) {
                call.future.completeExceptionally(new RpcException("RPC response header does not match request", null));
                return;
            }
            try {
                if (response.getErrorCode() != 0) {
                    call.future.completeExceptionally(new RpcException(Integer.toUnsignedLong(response.getErrorCode()), response.getErrorMessage()));
                } else {
                    call.complete(response.getPayload().toByteArray());
                }
            } catch (Exception e) {
                call.future.completeExceptionally(new RpcException("Invalid RPC response", e));
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            failPending(new RpcException("RPC connection closed", null));
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            failPending(new RpcException("RPC connection failed", cause));
            ctx.close();
        }

        private void failPending(Throwable error) {
            for (Pending<?> call : client.pending.values()) {
                if (client.pending.remove(call.requestId, call)) {
                    call.cancelTimeout();
                    call.future.completeExceptionally(error);
                }
            }
        }
    }

    private static final class Pending<Resp extends Message> {
        private final long requestId;
        private final long methodId;
        private final long logId;
        private final Parser<Resp> parser;
        private final CompletableFuture<Resp> future = new CompletableFuture<>();
        private volatile io.netty.util.concurrent.ScheduledFuture<?> timeout;
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
            io.netty.util.concurrent.ScheduledFuture<?> current = timeout;
            if (current != null) current.cancel(false);
        }
    }
}
