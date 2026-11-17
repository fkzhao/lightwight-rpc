package dev.yumix.rpc.example;

import dev.yumix.rpc.server.RpcServer;

import java.net.InetSocketAddress;

/** Runs the Echo service using package-based handler discovery. */
public final class EchoScanServerMain {
    private EchoScanServerMain() {}

    public static void main(String[] args) throws Exception {
        int port = args.length == 0 ? 9080 : Integer.parseInt(args[0]);
        RpcServer server = new RpcServer(new InetSocketAddress("0.0.0.0", port), "echo-server",
                Math.max(2, Runtime.getRuntime().availableProcessors()), 4 * 1024 * 1024);
        server.scanAndRegister(EchoScanServerMain.class);
        Runtime.getRuntime().addShutdownHook(new Thread(server::close));
        server.start();
        server.awaitTermination();
    }
}
