package dev.yumix.rpc;

import dev.yumix.rpc.client.RpcClient;
import dev.yumix.rpc.example.EchoRpc;
import dev.yumix.rpc.example.proto.EchoRequest;
import dev.yumix.rpc.example.proto.EchoResponse;
import dev.yumix.rpc.server.RpcServer;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class MultiplexingTest {
    @Test
    void matchesOutOfOrderRepliesWhenHeaderIdsCollide() throws Exception {
        CountDownLatch slowStarted = new CountDownLatch(1);
        CountDownLatch releaseSlow = new CountDownLatch(1);
        RpcServer server = new RpcServer(new InetSocketAddress("127.0.0.1", 0), "test-server", 2, 1024);
        server.register(EchoRpc.ECHO, request -> {
            if (request.getMessage().equals("slow")) {
                slowStarted.countDown();
                if (!releaseSlow.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("Slow call was not released");
            }
            return EchoResponse.newBuilder().setMessage(request.getMessage()).build();
        });
        server.start();
        try (RpcClient client = RpcClient.connect(server.localAddress(), "test-client", 10_000, 1024)) {
            CompletableFuture<EchoResponse> slow = client.call(EchoRpc.ECHO,
                    EchoRequest.newBuilder().setMessage("slow").build(), 7);
            assertTrue(slowStarted.await(3, TimeUnit.SECONDS));

            Field field = RpcClient.class.getDeclaredField("nextRequestId");
            field.setAccessible(true);
            AtomicLong sequence = (AtomicLong) field.get(client);
            long firstRequestId = sequence.get();
            long nextRequestId = sequence.addAndGet(65_535) + 1;
            assertEquals((int) firstRequestId & 0xffff, (int) nextRequestId & 0xffff);

            CompletableFuture<EchoResponse> fast = client.call(EchoRpc.ECHO,
                    EchoRequest.newBuilder().setMessage("fast").build(), 7);
            assertEquals("fast", fast.get(3, TimeUnit.SECONDS).getMessage());
            assertFalse(slow.isDone());
            releaseSlow.countDown();
            assertEquals("slow", slow.get(3, TimeUnit.SECONDS).getMessage());
        } finally {
            releaseSlow.countDown();
            server.close();
        }
    }
}
