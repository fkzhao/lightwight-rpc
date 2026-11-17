package dev.yumix.rpc;

import dev.yumix.rpc.client.RpcClient;
import dev.yumix.rpc.client.RpcClientConfig;
import dev.yumix.rpc.client.RpcException;
import dev.yumix.rpc.example.EchoRpc;
import dev.yumix.rpc.example.proto.EchoRequest;
import dev.yumix.rpc.example.proto.EchoResponse;
import dev.yumix.rpc.server.RpcServer;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class CapacityTest {
    @Test
    void clientCapsPendingCallsAndReleasesCapacityAfterResponse() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (RpcServer server = new RpcServer(new InetSocketAddress("127.0.0.1", 0),
                "test-server", 1, 1024)) {
            server.register(EchoRpc.ECHO, request -> {
                if (request.getMessage().equals("slow")) {
                    started.countDown();
                    if (!release.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("Timed out");
                }
                return EchoResponse.newBuilder().setMessage(request.getMessage()).build();
            });
            server.start();
            RpcClientConfig config = new RpcClientConfig(5_000, 1024,
                    1_000, 30_000, 10_000, 100, 1_000, 1);
            try (RpcClient client = RpcClient.connect(server.localAddress(), "test-client", config)) {
                CompletableFuture<EchoResponse> first = client.call(EchoRpc.ECHO, request("slow"));
                assertTrue(started.await(3, TimeUnit.SECONDS));
                ExecutionException rejected = assertThrows(ExecutionException.class,
                        () -> client.call(EchoRpc.ECHO, request("rejected")).get(2, TimeUnit.SECONDS));
                assertTrue(assertInstanceOf(RpcException.class, rejected.getCause())
                        .getMessage().contains("in-flight"));
                release.countDown();
                assertEquals("slow", first.get(3, TimeUnit.SECONDS).getMessage());
                assertEquals("next", client.call(EchoRpc.ECHO, request("next"))
                        .get(3, TimeUnit.SECONDS).getMessage());
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    void serverRejectsOverflowAndKeepsConnectionUsable() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (RpcServer server = new RpcServer(new InetSocketAddress("127.0.0.1", 0),
                "test-server", 1, 1024, 90_000, false, 1)) {
            server.register(EchoRpc.ECHO, request -> {
                if (request.getMessage().equals("slow")) {
                    started.countDown();
                    if (!release.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("Timed out");
                }
                return EchoResponse.newBuilder().setMessage(request.getMessage()).build();
            });
            server.start();
            try (RpcClient client = RpcClient.connect(server.localAddress(), "test-client", 5_000, 1024)) {
                CompletableFuture<EchoResponse> first = client.call(EchoRpc.ECHO, request("slow"));
                assertTrue(started.await(3, TimeUnit.SECONDS));
                List<CompletableFuture<EchoResponse>> overflow = new ArrayList<>();
                for (int i = 0; i < 10; i++) overflow.add(client.call(EchoRpc.ECHO, request("queued-" + i)));
                for (int i = 1; i < overflow.size(); i++) {
                    CompletableFuture<EchoResponse> call = overflow.get(i);
                    ExecutionException rejected = assertThrows(ExecutionException.class,
                            () -> call.get(2, TimeUnit.SECONDS));
                    assertEquals(4, assertInstanceOf(RpcException.class, rejected.getCause()).errorCode());
                }
                release.countDown();
                assertEquals("slow", first.get(3, TimeUnit.SECONDS).getMessage());
                assertEquals("queued-0", overflow.get(0).get(3, TimeUnit.SECONDS).getMessage());
                assertEquals("after", client.call(EchoRpc.ECHO, request("after"))
                        .get(3, TimeUnit.SECONDS).getMessage());
            } finally {
                release.countDown();
            }
        }
    }

    private static EchoRequest request(String message) {
        return EchoRequest.newBuilder().setMessage(message).build();
    }
}
