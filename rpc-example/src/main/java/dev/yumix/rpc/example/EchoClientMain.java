package dev.yumix.rpc.example;

import dev.yumix.rpc.client.RpcClient;
import dev.yumix.rpc.example.proto.EchoRequest;
import dev.yumix.rpc.example.proto.EchoResponse;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

public final class EchoClientMain {
    private EchoClientMain() {}

    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "127.0.0.1";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 9080;
        String message = args.length > 2 ? args[2] : "Hello, nshead + protobuf!";
        try (RpcClient client = RpcClient.connect(new InetSocketAddress(host, port), "echo-client",
                3000, 4 * 1024 * 1024)) {
            EchoResponse response = client.call(EchoRpc.ECHO,
                    EchoRequest.newBuilder().setMessage(message).build(), 1)
                    .get(4, TimeUnit.SECONDS);
            System.out.println(response.getMessage());
        }
    }
}
