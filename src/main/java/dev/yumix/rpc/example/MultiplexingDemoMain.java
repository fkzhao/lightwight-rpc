package dev.yumix.rpc.example;

import dev.yumix.rpc.example.proto.EchoRequest;
import dev.yumix.rpc.server.RpcServer;

import java.net.InetSocketAddress;

/** Starts an in-process server and runs the client demo over real TCP. */
public final class MultiplexingDemoMain {
    private MultiplexingDemoMain() {}

    public static void main(String[] args) throws Exception {
        try (RpcServer server = new RpcServer(new InetSocketAddress("127.0.0.1", 0),
                "demo-server", 4, 1024 * 1024)) {
            server.registerWithContext(EchoServerMain.ECHO_METHOD_ID, EchoRequest.parser(), EchoService::handle);
            server.start();
            MultiplexingClientMain.run(server.localAddress());
        }
    }
}
