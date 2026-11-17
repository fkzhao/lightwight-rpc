package dev.yumix.rpc.example;

import dev.yumix.rpc.client.RpcClientPool;
import dev.yumix.rpc.example.proto.EchoRequest;
import dev.yumix.rpc.example.proto.EchoResponse;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/** Calls the route registered from the nested example package. */
public final class NestedEchoClientMain {
    private NestedEchoClientMain() {}

    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "127.0.0.1";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 9080;
        String message = args.length > 2 ? args[2] : "hello";
        try (RpcClientPool pool = RpcClientPool.builder(new InetSocketAddress(host, port),
                "nested-client").build()) {
            EchoResponse response = pool.call(EchoRpc.NESTED_ECHO,
                    EchoRequest.newBuilder().setMessage(message).build())
                    .get(20, TimeUnit.SECONDS);
            System.out.println(response.getMessage());
        }
    }
}
