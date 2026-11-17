package dev.yumix.rpc.example;

import dev.yumix.rpc.client.RpcClient;
import dev.yumix.rpc.example.proto.EchoRequest;
import dev.yumix.rpc.example.proto.EchoResponse;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Sends concurrent requests through one client TCP connection. */
public final class MultiplexingClientMain {
    private MultiplexingClientMain() {}

    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "127.0.0.1";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 9080;
        run(new InetSocketAddress(host, port));
    }

    static void run(InetSocketAddress address) throws Exception {
        List<Job> jobs = List.of(
                new Job("A-slow", 1500),
                new Job("B-fast", 100),
                new Job("C-medium", 700),
                new Job("D-quick", 300));
        long startedAt = System.nanoTime();
        try (RpcClient client = RpcClient.connect(address, "demo-client", 5000, 1024 * 1024)) {
            System.out.printf("One TCP connection: client %s -> server %s%n",
                    client.localAddress(), client.remoteAddress());
            List<CompletableFuture<Void>> observations = new ArrayList<>();
            ConcurrentLinkedQueue<String> completionOrder = new ConcurrentLinkedQueue<>();
            AtomicInteger completionNumber = new AtomicInteger();

            for (int i = 0; i < jobs.size(); i++) {
                Job job = jobs.get(i);
                System.out.printf("%4d ms  SEND   #%d  request=%s  delay=%d ms  connection=%s -> %s%n",
                        elapsedMs(startedAt), i + 1, job.name(), job.delayMs(),
                        client.localAddress(), client.remoteAddress());
                EchoRequest request = EchoRequest.newBuilder()
                        .setMessage(job.name()).setDelayMs(job.delayMs()).build();
                CompletableFuture<Void> observation = client.request(EchoServerMain.ECHO_METHOD_ID, 42,
                                request, EchoResponse.parser())
                        .thenAccept(response -> {
                            completionOrder.add(response.getMessage());
                            System.out.printf("%4d ms  RECV   #%d  response=%s  connection=%s -> %s%n",
                                    elapsedMs(startedAt), completionNumber.incrementAndGet(),
                                    response.getMessage(), client.localAddress(), client.remoteAddress());
                        });
                observations.add(observation);
            }

            CompletableFuture.allOf(observations.toArray(CompletableFuture[]::new))
                    .get(6, TimeUnit.SECONDS);
            System.out.println("Send order:       " + jobs.stream().map(Job::name).toList());
            System.out.println("Completion order: " + completionOrder);
            System.out.printf("Elapsed: %d ms over one TCP connection%n", elapsedMs(startedAt));
        }
    }

    private static long elapsedMs(long startedAt) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);
    }

    private record Job(String name, int delayMs) {}
}
