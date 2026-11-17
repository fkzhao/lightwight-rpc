package dev.yumix.rpc.benchmark;

import dev.yumix.rpc.example.EchoRpc;
import dev.yumix.rpc.example.proto.EchoResponse;
import dev.yumix.rpc.server.RpcServer;

import java.net.InetSocketAddress;

/** Echo RPC server without per-request INFO logging for external load tests. */
public final class BenchmarkServerMain {
    private BenchmarkServerMain() {}

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 9081;
        int workers = args.length > 1 ? Integer.parseInt(args[1])
                : Runtime.getRuntime().availableProcessors();
        RpcServer server = new RpcServer(new InetSocketAddress("0.0.0.0", port),
                "bench-server", workers, 4 * 1024 * 1024, 90_000, false);
        server.register(EchoRpc.ECHO, request ->
                EchoResponse.newBuilder().setMessage(request.getMessage()).build());
        Runtime.getRuntime().addShutdownHook(new Thread(server::close));
        server.start();
        System.out.println("Benchmark server listening on port " + port + " with " + workers + " workers");
        server.awaitTermination();
    }
}
