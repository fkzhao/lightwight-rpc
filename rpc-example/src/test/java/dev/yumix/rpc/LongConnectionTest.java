package dev.yumix.rpc;

import dev.yumix.rpc.client.RpcClient;
import dev.yumix.rpc.client.RpcClientConfig;
import dev.yumix.rpc.example.proto.EchoRequest;
import dev.yumix.rpc.example.proto.EchoResponse;
import dev.yumix.rpc.server.RpcServer;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

final class LongConnectionTest {
    @Test
    void heartbeatKeepsAnIdleConnectionOpen() throws Exception {
        RpcClientConfig config = new RpcClientConfig(2_000, 1024, 500, 100, 200, 50, 500);
        try (RpcServer server = new RpcServer(new InetSocketAddress("127.0.0.1", 0),
                "test-server", 2, 1024, 500)) {
            server.register(1, EchoRequest.parser(), request ->
                    EchoResponse.newBuilder().setMessage(request.getMessage()).build());
            server.start();
            try (RpcClient client = RpcClient.connect(server.localAddress(), "test-client", config)) {
                SocketAddress localAddress = client.localAddress();
                Thread.sleep(1_100);
                assertTrue(client.isConnected());
                assertEquals(localAddress, client.localAddress());
                assertEquals("alive", client.request(1, 1,
                        EchoRequest.newBuilder().setMessage("alive").build(), EchoResponse.parser())
                        .get(2, TimeUnit.SECONDS).getMessage());
            }
        }
    }

    @Test
    void reconnectsAfterServerRestartWithoutReplayingPendingCalls() throws Exception {
        RpcClientConfig config = new RpcClientConfig(3_000, 1024, 500, 30_000, 1_000, 50, 500);
        CountDownLatch requestStarted = new CountDownLatch(1);
        CountDownLatch releaseRequest = new CountDownLatch(1);
        RpcServer first = new RpcServer(new InetSocketAddress("127.0.0.1", 0),
                "test-server", 2, 1024);
        first.register(1, EchoRequest.parser(), request -> {
            requestStarted.countDown();
            releaseRequest.await(10, TimeUnit.SECONDS);
            return EchoResponse.newBuilder().setMessage(request.getMessage()).build();
        });
        first.start();
        InetSocketAddress address = first.localAddress();
        RpcServer second = null;
        try (RpcClient client = RpcClient.connect(address, "test-client", config)) {
            CompletableFuture<EchoResponse> pending = client.request(1, 1,
                    EchoRequest.newBuilder().setMessage("before").build(), EchoResponse.parser());
            assertTrue(requestStarted.await(3, TimeUnit.SECONDS));
            first.close();
            assertThrows(ExecutionException.class, () -> pending.get(3, TimeUnit.SECONDS));
            awaitState(() -> !client.isConnected(), 3_000);
            assertThrows(ExecutionException.class, () -> client.request(1, 2,
                    EchoRequest.newBuilder().setMessage("while-down").build(), EchoResponse.parser())
                    .get(1, TimeUnit.SECONDS));

            second = new RpcServer(address, "test-server", 2, 1024);
            second.register(1, EchoRequest.parser(), request ->
                    EchoResponse.newBuilder().setMessage(request.getMessage()).build());
            second.start();
            awaitState(client::isConnected, 8_000);
            assertEquals("after", client.request(1, 2,
                    EchoRequest.newBuilder().setMessage("after").build(), EchoResponse.parser())
                    .get(3, TimeUnit.SECONDS).getMessage());
        } finally {
            releaseRequest.countDown();
            if (second != null) second.close();
            first.close();
        }
    }

    @Test
    void openConnectsWhenServerStartsLater() throws Exception {
        int port;
        try (ServerSocket reservation = new ServerSocket(0)) {
            port = reservation.getLocalPort();
        }
        InetSocketAddress address = new InetSocketAddress("127.0.0.1", port);
        RpcClientConfig config = new RpcClientConfig(2_000, 1024, 500, 30_000, 1_000, 50, 500);
        try (RpcClient client = RpcClient.open(address, "test-client", config);
             RpcServer server = new RpcServer(address, "test-server", 2, 1024)) {
            Thread.sleep(100);
            assertFalse(client.isConnected());
            server.register(1, EchoRequest.parser(), request ->
                    EchoResponse.newBuilder().setMessage(request.getMessage()).build());
            server.start();
            awaitState(client::isConnected, 5_000);
            assertEquals("ready", client.request(1, 3,
                    EchoRequest.newBuilder().setMessage("ready").build(), EchoResponse.parser())
                    .get(2, TimeUnit.SECONDS).getMessage());
        }
    }

    private static void awaitState(BooleanSupplier condition, long timeoutMillis) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadline) fail("Timed out waiting for connection state");
            Thread.sleep(20);
        }
    }
}
