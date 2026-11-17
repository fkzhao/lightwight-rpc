package io.github.fkzhao.lightweightrpc.example;

import io.github.fkzhao.lightweightrpc.example.proto.EchoRequest;
import io.github.fkzhao.lightweightrpc.example.proto.EchoResponse;
import io.github.fkzhao.lightweightrpc.server.RpcServer;

import java.net.InetSocketAddress;

public final class EchoServerMain {
    public static final long ECHO_METHOD_ID = 1;

    private EchoServerMain() {}

    public static void main(String[] args) throws Exception {
        int port = args.length == 0 ? 9000 : Integer.parseInt(args[0]);
        RpcServer server = new RpcServer(new InetSocketAddress("0.0.0.0", port), "echo-server",
                Math.max(2, Runtime.getRuntime().availableProcessors()), 4 * 1024 * 1024);
        server.register(ECHO_METHOD_ID, EchoRequest.parser(), request ->
                EchoResponse.newBuilder().setMessage(request.getMessage()).build());
        Runtime.getRuntime().addShutdownHook(new Thread(server::close));
        server.start();
        System.out.println("Echo RPC server listening on port " + port);
        server.awaitTermination();
    }
}
