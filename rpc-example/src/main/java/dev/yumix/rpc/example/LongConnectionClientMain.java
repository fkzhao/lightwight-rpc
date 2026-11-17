package dev.yumix.rpc.example;

import dev.yumix.rpc.client.RpcClient;
import dev.yumix.rpc.example.proto.EchoRequest;
import dev.yumix.rpc.example.proto.EchoResponse;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/** Continuously sends Echo calls while the client reconnects in the background. */
public final class LongConnectionClientMain {
    private LongConnectionClientMain() {}

    public static void main(String[] args) throws InterruptedException {
        String host = args.length > 0 ? args[0] : "127.0.0.1";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 9080;
        InetSocketAddress address = new InetSocketAddress(host, port);
        try (RpcClient client = RpcClient.open(address, "long-client", 3_000, 4 * 1024 * 1024)) {
            Runtime.getRuntime().addShutdownHook(new Thread(client::close));
            System.out.println("Long-lived RPC client started; restart the Echo server to observe reconnection.");
            boolean wasConnected = false;
            long sequence = 0;
            while (!Thread.currentThread().isInterrupted()) {
                boolean connected = client.isConnected();
                if (connected != wasConnected) {
                    System.out.printf("Connection state: %s  local=%s  remote=%s%n",
                            connected ? "CONNECTED" : "DISCONNECTED", client.localAddress(), address);
                    wasConnected = connected;
                }
                if (connected) {
                    String message = "long-call-" + ++sequence;
                    System.out.println("SEND " + message);
                    client.request(EchoServerMain.ECHO_METHOD_ID, sequence,
                            EchoRequest.newBuilder().setMessage(message).build(), EchoResponse.parser())
                            .whenComplete((response, error) -> {
                                if (error == null) System.out.println("RECV " + response.getMessage());
                                else System.out.println("FAILED " + message + ": " + error.getMessage());
                            });
                }
                TimeUnit.SECONDS.sleep(2);
            }
        }
    }
}
