package dev.yumix.rpc;

import dev.yumix.rpc.badscanfixture.BadScanAnchor;
import dev.yumix.rpc.client.RpcClient;
import dev.yumix.rpc.example.EchoRpc;
import dev.yumix.rpc.example.EchoScanServerMain;
import dev.yumix.rpc.example.proto.EchoRequest;
import dev.yumix.rpc.example.proto.EchoResponse;
import dev.yumix.rpc.scanfixture.ScanAnchor;
import dev.yumix.rpc.scanfixture.nested.ScannedEchoHandler;
import dev.yumix.rpc.server.RpcServer;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ScanRegistrationTest {
    @Test
    void findsAnnotatedHandlerInAnchorPackage() throws Exception {
        try (RpcServer server = server()) {
            server.scanAndRegister(EchoScanServerMain.class).start();
            try (RpcClient client = RpcClient.connect(server.localAddress(), "test-client", 2_000, 1024)) {
                assertEquals("hello", client.call(EchoRpc.ECHO,
                        EchoRequest.newBuilder().setMessage("hello").build())
                        .get(2, TimeUnit.SECONDS).getMessage());
            }
        }
    }

    @Test
    void scansSubpackageAndUsesCustomInstanceFactory() throws Exception {
        AtomicInteger created = new AtomicInteger();
        RpcMethodDescriptor<EchoRequest, EchoResponse> scanned = new RpcMethodDescriptor<>(
                703, EchoRequest.parser(), EchoResponse.parser());
        try (RpcServer server = server()) {
            server.scanAndRegister(ScanAnchor.class, type -> {
                created.incrementAndGet();
                assertEquals(ScannedEchoHandler.class, type);
                return new ScannedEchoHandler();
            });
            server.register(704, EchoRequest.parser(), request ->
                    EchoResponse.newBuilder().setMessage("manual:" + request.getMessage()).build());
            server.start();
            try (RpcClient client = RpcClient.connect(server.localAddress(), "test-client", 2_000, 1024)) {
                assertEquals("scanned:one", client.call(scanned,
                        EchoRequest.newBuilder().setMessage("one").build())
                        .get(2, TimeUnit.SECONDS).getMessage());
                assertEquals("manual:two", client.request(704, 0,
                        EchoRequest.newBuilder().setMessage("two").build(), EchoResponse.parser())
                        .get(2, TimeUnit.SECONDS).getMessage());
            }
            assertEquals(1, created.get());
        }
    }

    @Test
    void rejectsDuplicateAndInvalidRoutesBeforeBinding() {
        try (RpcServer duplicate = server(); RpcServer invalid = server()) {
            duplicate.register(703, EchoRequest.parser(), request ->
                    EchoResponse.newBuilder().setMessage(request.getMessage()).build());
            IllegalArgumentException collision = assertThrows(IllegalArgumentException.class,
                    () -> duplicate.scanAndRegister(ScanAnchor.class));
            assertTrue(collision.getMessage().contains("703"));
            IllegalArgumentException badSignature = assertThrows(IllegalArgumentException.class,
                    () -> invalid.scanAndRegister(BadScanAnchor.class));
            assertTrue(badSignature.getMessage().contains("Invalid RPC route"));
        }
    }

    private static RpcServer server() {
        return new RpcServer(new InetSocketAddress("127.0.0.1", 0), "test-server", 2, 1024);
    }
}
