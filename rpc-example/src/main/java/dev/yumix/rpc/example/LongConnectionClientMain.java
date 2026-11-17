package dev.yumix.rpc.example;

import dev.yumix.rpc.client.RpcClientPool;
import dev.yumix.rpc.client.RpcPoolState;
import dev.yumix.rpc.example.proto.EchoRequest;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/** Continuously sends Echo calls while the client reconnects in the background. */
public final class LongConnectionClientMain {
    private LongConnectionClientMain() {}

    public static void main(String[] args) throws InterruptedException {
        String host = args.length > 0 ? args[0] : "127.0.0.1";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 9080;
        InetSocketAddress address = new InetSocketAddress(host, port);
        try (RpcClientPool pool = RpcClientPool.builder(address, "long-client").build()) {
            Runtime.getRuntime().addShutdownHook(new Thread(pool::close));
            System.out.println("Long-lived RPC client started; restart the Echo server to observe reconnection.");
            RpcPoolState previousState = null;
            long sequence = 0;
            while (!Thread.currentThread().isInterrupted()) {
                RpcPoolState currentState = pool.state();
                if (currentState != previousState) {
                    System.out.printf("Pool state: %s  connections=%s  remote=%s%n",
                            currentState, pool.stats().connections(), address);
                    previousState = currentState;
                }
                String message = "long-call-" + ++sequence;
                System.out.println("SEND " + message);
                pool.call(EchoRpc.ECHO,
                        EchoRequest.newBuilder().setMessage(message).build(), sequence)
                        .whenComplete((response, error) -> {
                            if (error == null) System.out.println("RECV " + response.getMessage());
                            else System.out.println("FAILED " + message + ": " + error.getMessage());
                        });
                TimeUnit.SECONDS.sleep(2);
            }
        }
    }
}
