package dev.yumix.rpc;

import dev.yumix.rpc.client.ConnectionUnavailableException;
import dev.yumix.rpc.client.LocalOverloadException;
import dev.yumix.rpc.client.OutcomeUnknownException;
import dev.yumix.rpc.client.RpcClientPool;
import dev.yumix.rpc.client.RpcPoolState;
import dev.yumix.rpc.client.RpcTimeoutException;
import dev.yumix.rpc.example.EchoRpc;
import dev.yumix.rpc.example.proto.EchoRequest;
import dev.yumix.rpc.example.proto.EchoResponse;
import dev.yumix.rpc.server.RpcServer;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

final class RpcClientPoolTest {
    @Test
    void queuesAnUnsentCallAndMultiplexesOutOfOrderOnOneConnection() throws Exception {
        int port = freePort();
        InetSocketAddress address = new InetSocketAddress("127.0.0.1", port);
        CountDownLatch slowStarted = new CountDownLatch(1);
        CountDownLatch releaseSlow = new CountDownLatch(1);
        AtomicReference<String> slowConnection = new AtomicReference<>();
        AtomicReference<String> fastConnection = new AtomicReference<>();
        try (RpcClientPool pool = fastPool(address).maxReconnectAttempts(20).build();
             RpcServer server = new RpcServer(address, "test-server", 2, 1024)) {
            CompletableFuture<EchoResponse> queued = pool.call(EchoRpc.ECHO, request("queued"));
            assertEquals(1, pool.stats().queuedCalls());
            server.registerWithContext(EchoRpc.ECHO, (request, context) -> {
                if (request.getMessage().equals("slow")) {
                    slowConnection.set(context.connectionId());
                    slowStarted.countDown();
                    if (!releaseSlow.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("Timed out");
                } else if (request.getMessage().equals("fast")) {
                    fastConnection.set(context.connectionId());
                }
                return EchoResponse.newBuilder().setMessage(request.getMessage()).build();
            });
            server.start();
            assertEquals("queued", queued.get(5, TimeUnit.SECONDS).getMessage());
            pool.ready().get(2, TimeUnit.SECONDS);
            CompletableFuture<EchoResponse> slow = pool.call(EchoRpc.ECHO, request("slow"));
            assertTrue(slowStarted.await(2, TimeUnit.SECONDS));
            assertEquals("fast", pool.call(EchoRpc.ECHO, request("fast"))
                    .get(2, TimeUnit.SECONDS).getMessage());
            assertFalse(slow.isDone());
            releaseSlow.countDown();
            assertEquals("slow", slow.get(2, TimeUnit.SECONDS).getMessage());
            assertEquals(slowConnection.get(), fastConnection.get());
            assertEquals(1, pool.stats().readyConnections());
            assertEquals(0, pool.stats().queuedCalls());
        } finally {
            releaseSlow.countDown();
        }
    }

    @Test
    void failsAfterReconnectThresholdThenRecoversInBackground() throws Exception {
        InetSocketAddress address = new InetSocketAddress("127.0.0.1", freePort());
        try (RpcClientPool pool = fastPool(address).maxReconnectAttempts(2).build()) {
            CompletableFuture<EchoResponse> waiting = pool.call(EchoRpc.ECHO, request("never-sent"));
            ExecutionException error = assertThrows(ExecutionException.class,
                    () -> waiting.get(4, TimeUnit.SECONDS));
            assertInstanceOf(ConnectionUnavailableException.class, error.getCause());
            assertEquals(RpcPoolState.UNAVAILABLE, pool.state());
            assertEquals(0, pool.stats().queuedCalls());
            ExecutionException immediate = assertThrows(ExecutionException.class,
                    () -> pool.call(EchoRpc.ECHO, request("reject")).get(1, TimeUnit.SECONDS));
            assertInstanceOf(ConnectionUnavailableException.class, immediate.getCause());

            try (RpcServer server = new RpcServer(address, "test-server", 2, 1024)) {
                server.register(EchoRpc.ECHO, request ->
                        EchoResponse.newBuilder().setMessage(request.getMessage()).build());
                server.start();
                await(() -> pool.state() == RpcPoolState.READY, 5_000);
                assertEquals("recovered", pool.call(EchoRpc.ECHO, request("recovered"))
                        .get(2, TimeUnit.SECONDS).getMessage());
            }
        }
    }

    @Test
    void disconnectFailsWrittenCallButSendsQueuedCallAfterReconnect() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        RpcServer first = new RpcServer(new InetSocketAddress("127.0.0.1", 0),
                "test-server", 2, 1024);
        first.register(EchoRpc.ECHO, request -> {
            started.countDown();
            release.await(5, TimeUnit.SECONDS);
            return EchoResponse.newBuilder().setMessage(request.getMessage()).build();
        });
        first.start();
        InetSocketAddress address = first.localAddress();
        try (RpcClientPool pool = fastPool(address).maxReconnectAttempts(100)
                .connectionWaitTimeout(Duration.ofSeconds(10)).build()) {
            pool.ready().get(2, TimeUnit.SECONDS);
            CompletableFuture<EchoResponse> written = pool.call(EchoRpc.ECHO, request("written"));
            assertTrue(started.await(2, TimeUnit.SECONDS));
            first.close();
            ExecutionException error = assertThrows(ExecutionException.class,
                    () -> written.get(3, TimeUnit.SECONDS));
            assertInstanceOf(OutcomeUnknownException.class, error.getCause());
            await(() -> pool.stats().readyConnections() == 0, 2_000);
            CompletableFuture<EchoResponse> queued = pool.call(EchoRpc.ECHO, request("queued"));
            try (RpcServer replacement = new RpcServer(address, "test-server", 2, 1024)) {
                replacement.register(EchoRpc.ECHO, request ->
                        EchoResponse.newBuilder().setMessage(request.getMessage()).build());
                replacement.start();
                assertEquals("queued", queued.get(10, TimeUnit.SECONDS).getMessage());
            }
        } finally {
            release.countDown();
            first.close();
        }
    }

    @Test
    void enforcesInFlightAndQueueLimitsAndSeparatesCallbackFromIoThread() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (RpcServer server = new RpcServer(new InetSocketAddress("127.0.0.1", 0),
                "test-server", 2, 1024)) {
            server.register(EchoRpc.ECHO, request -> {
                if (request.getMessage().equals("slow")) {
                    started.countDown();
                    release.await(5, TimeUnit.SECONDS);
                }
                return EchoResponse.newBuilder().setMessage(request.getMessage()).build();
            });
            server.start();
            try (RpcClientPool pool = fastPool(server.localAddress())
                    .maxInFlightPerConnection(1).build()) {
                pool.ready().get(2, TimeUnit.SECONDS);
                CompletableFuture<EchoResponse> slow = pool.call(EchoRpc.ECHO, request("slow"));
                assertTrue(started.await(2, TimeUnit.SECONDS));
                ExecutionException overloaded = assertThrows(ExecutionException.class,
                        () -> pool.call(EchoRpc.ECHO, request("other")).get(1, TimeUnit.SECONDS));
                assertInstanceOf(LocalOverloadException.class, overloaded.getCause());
                release.countDown();
                assertEquals("slow", slow.get(2, TimeUnit.SECONDS).getMessage());
                CompletableFuture<String> callbackThread = pool.call(EchoRpc.ECHO, request("next"))
                        .thenApply(response -> Thread.currentThread().getName());
                assertFalse(callbackThread.get(2, TimeUnit.SECONDS).startsWith("rpc-pool-io"));
            } finally {
                release.countDown();
            }
        }

        try (RpcClientPool unavailable = fastPool(new InetSocketAddress("127.0.0.1", freePort()))
                .maxQueuedCalls(1).maxReconnectAttempts(20).build()) {
            CompletableFuture<EchoResponse> queued = unavailable.call(EchoRpc.ECHO, request("one"));
            ExecutionException full = assertThrows(ExecutionException.class,
                    () -> unavailable.call(EchoRpc.ECHO, request("two")).get(1, TimeUnit.SECONDS));
            assertInstanceOf(LocalOverloadException.class, full.getCause());
            queued.cancel(true);
            await(() -> unavailable.stats().queuedCalls() == 0, 2_000);
        }
    }

    @Test
    void responseTimeoutDiscardsLateReply() throws Exception {
        try (RpcServer server = new RpcServer(new InetSocketAddress("127.0.0.1", 0),
                "test-server", 2, 1024)) {
            server.register(EchoRpc.ECHO, request -> {
                if (request.getMessage().equals("slow")) Thread.sleep(250);
                return EchoResponse.newBuilder().setMessage(request.getMessage()).build();
            });
            server.start();
            try (RpcClientPool pool = fastPool(server.localAddress())
                    .responseTimeout(Duration.ofMillis(80)).build()) {
                pool.ready().get(2, TimeUnit.SECONDS);
                ExecutionException timedOut = assertThrows(ExecutionException.class,
                        () -> pool.call(EchoRpc.ECHO, request("slow")).get(2, TimeUnit.SECONDS));
                assertEquals(RpcTimeoutException.Stage.RESPONSE,
                        assertInstanceOf(RpcTimeoutException.class, timedOut.getCause()).stage());
                await(() -> pool.stats().discardedLateResponses() > 0, 2_000);
                assertEquals("fast", pool.call(EchoRpc.ECHO, request("fast"))
                        .get(2, TimeUnit.SECONDS).getMessage());
            }
        }
    }

    @Test
    void connectionWaitTimeoutAndByteLimitAreDistinct() throws Exception {
        InetSocketAddress address = new InetSocketAddress("127.0.0.1", freePort());
        try (RpcClientPool pool = fastPool(address).maxReconnectAttempts(100)
                .connectionWaitTimeout(Duration.ofMillis(120))
                .maxQueuedBytes(64).build()) {
            CompletableFuture<EchoResponse> waiting = pool.call(EchoRpc.ECHO, request("small"));
            ExecutionException tooLarge = assertThrows(ExecutionException.class,
                    () -> pool.call(EchoRpc.ECHO, request("x".repeat(100)))
                            .get(1, TimeUnit.SECONDS));
            assertInstanceOf(LocalOverloadException.class, tooLarge.getCause());
            ExecutionException timedOut = assertThrows(ExecutionException.class,
                    () -> waiting.get(2, TimeUnit.SECONDS));
            assertEquals(RpcTimeoutException.Stage.CONNECTION_WAIT,
                    assertInstanceOf(RpcTimeoutException.class, timedOut.getCause()).stage());
            assertEquals(0, pool.stats().queuedCalls());
        }
    }

    @Test
    void closesAConnectionWhenPingGetsNoPong() throws Exception {
        try (ServerSocket blackhole = new ServerSocket(0)) {
            CountDownLatch accepted = new CountDownLatch(1);
            Thread acceptor = new Thread(() -> {
                try (Socket ignored = blackhole.accept()) {
                    accepted.countDown();
                    Thread.sleep(1_000);
                } catch (Exception ignored) {
                    accepted.countDown();
                }
            }, "silent-rpc-server");
            acceptor.setDaemon(true);
            acceptor.start();
            try (RpcClientPool pool = fastPool(new InetSocketAddress("127.0.0.1",
                    blackhole.getLocalPort())).heartbeatIdle(Duration.ofMillis(100))
                    .heartbeatTimeout(Duration.ofMillis(120)).build()) {
                pool.ready().get(2, TimeUnit.SECONDS);
                assertTrue(accepted.await(2, TimeUnit.SECONDS));
                await(() -> pool.stats().heartbeatTimeouts() > 0, 2_000);
            }
        }
    }

    private static RpcClientPool.Builder fastPool(InetSocketAddress address) {
        return RpcClientPool.builder(address, "test-client")
                .maxBodyLength(1024)
                .connectTimeout(Duration.ofMillis(250))
                .reconnectBackoff(Duration.ofMillis(50), Duration.ofMillis(200))
                .connectionWaitTimeout(Duration.ofSeconds(5))
                .responseTimeout(Duration.ofSeconds(2));
    }

    private static EchoRequest request(String value) {
        return EchoRequest.newBuilder().setMessage(value).build();
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) { return socket.getLocalPort(); }
    }

    private static void await(BooleanSupplier condition, long timeoutMillis) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) fail("Timed out waiting for state");
            Thread.sleep(10);
        }
    }
}
