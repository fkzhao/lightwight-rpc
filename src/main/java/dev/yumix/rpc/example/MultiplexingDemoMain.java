package dev.yumix.rpc.example;

import dev.yumix.rpc.example.proto.EchoRequest;
import dev.yumix.rpc.server.RpcServer;

import java.net.InetSocketAddress;

/** Connects to an existing Echo server, or starts an embedded server on request. */
public final class MultiplexingDemoMain {
    private MultiplexingDemoMain() {}

    public static void main(String[] args) throws Exception {
        if (args.length == 0 || !"--standalone".equals(args[0])) {
            MultiplexingClientMain.main(args);
            return;
        }
        if (args.length != 1) throw new IllegalArgumentException("--standalone takes no other arguments");
        try (RpcServer server = new RpcServer(new InetSocketAddress("127.0.0.1", 0),
                "demo-server", 4, 1024 * 1024)) {
            server.registerWithContext(EchoServerMain.ECHO_METHOD_ID, EchoRequest.parser(), EchoService::handle);
            server.start();
            MultiplexingClientMain.run(server.localAddress());
        }
    }
}
