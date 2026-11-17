package dev.yumix.rpc.benchmark;

import dev.yumix.rpc.client.RpcClient;
import dev.yumix.rpc.example.EchoRpc;
import dev.yumix.rpc.example.proto.EchoRequest;
import dev.yumix.rpc.example.proto.EchoResponse;
import dev.yumix.rpc.server.RpcServer;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;

/** Closed-loop server load generator with bounded concurrency and latency percentiles. */
public final class LoadTestMain {
    private static final int MAX_BODY_LENGTH = 4 * 1024 * 1024;
    private static final int LATENCY_BUCKETS = 100_001;

    private LoadTestMain() {}

    public static void main(String[] args) throws Exception {
        Options options = Options.parse(args);
        RpcServer embeddedServer = null;
        List<RpcClient> clients = new ArrayList<>();
        try {
            InetSocketAddress address;
            if (options.host() == null) {
                embeddedServer = new RpcServer(new InetSocketAddress("127.0.0.1", 0),
                        "bench-server", options.serverWorkers(), MAX_BODY_LENGTH, 90_000, false);
                embeddedServer.register(EchoRpc.ECHO, request ->
                        EchoResponse.newBuilder().setMessage(request.getMessage()).build());
                embeddedServer.start();
                address = embeddedServer.localAddress();
            } else {
                address = new InetSocketAddress(options.host(), options.port());
            }
            for (int i = 0; i < options.connections(); i++) {
                clients.add(RpcClient.connect(address, "bench-client", 5_000, MAX_BODY_LENGTH));
            }
            runLoad(options, address, clients);
        } finally {
            for (RpcClient client : clients) client.close();
            if (embeddedServer != null) embeddedServer.close();
        }
    }

    private static void runLoad(Options options, InetSocketAddress address, List<RpcClient> clients)
            throws InterruptedException {
        EchoRequest request = EchoRequest.newBuilder()
                .setMessage("x".repeat(options.payloadBytes())).build();
        int slots = options.connections() * options.inflightPerConnection();
        ExecutorService launchers = Executors.newFixedThreadPool(
                Math.min(16, Math.max(2, Runtime.getRuntime().availableProcessors())));
        Metrics metrics = new Metrics();
        CountDownLatch finished = new CountDownLatch(slots);
        long startedAt = System.nanoTime();
        long measureAt = startedAt + TimeUnit.SECONDS.toNanos(options.warmupSeconds());
        long stopAt = measureAt + TimeUnit.SECONDS.toNanos(options.durationSeconds());

        System.out.printf("profile=%s mode=%s server=%s connections=%d inflightPerConnection=%d "
                        + "maxInflight=%d messageBytes=%d warmupSeconds=%d durationSeconds=%d%n",
                options.profile(), options.host() == null ? "embedded" : "remote", address,
                options.connections(), options.inflightPerConnection(), slots,
                options.payloadBytes(), options.warmupSeconds(), options.durationSeconds());
        System.out.printf("java=%s processors=%d%n", System.getProperty("java.version"),
                Runtime.getRuntime().availableProcessors());

        try {
            for (RpcClient client : clients) {
                for (int i = 0; i < options.inflightPerConnection(); i++) {
                    launchers.execute(new Slot(client, request, measureAt, stopAt, metrics, finished, launchers));
                }
            }
            long waitSeconds = options.warmupSeconds() + options.durationSeconds() + 15L;
            if (!finished.await(waitSeconds, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Load test did not drain within " + waitSeconds + " seconds");
            }
        } finally {
            launchers.shutdownNow();
        }

        long successes = metrics.success.sum();
        long errors = metrics.errors.sum();
        double throughput = successes / (double) options.durationSeconds();
        System.out.printf(Locale.ROOT,
                "success=%d errors=%d throughput=%.1f req/s p50=%.2f ms p95=%.2f ms "
                        + "p99=%.2f ms max=%.2f ms%n",
                successes, errors, throughput, metrics.percentile(0.50), metrics.percentile(0.95),
                metrics.percentile(0.99), metrics.maxNanos.get() / 1_000_000.0);
        if (successes == 0 || errors != 0) {
            throw new IllegalStateException("Load test failed: success=" + successes + ", errors=" + errors);
        }
    }

    private static final class Slot implements Runnable {
        private final RpcClient client;
        private final EchoRequest request;
        private final long measureAt;
        private final long stopAt;
        private final Metrics metrics;
        private final CountDownLatch finished;
        private final ExecutorService launchers;

        private Slot(RpcClient client, EchoRequest request, long measureAt, long stopAt,
                     Metrics metrics, CountDownLatch finished, ExecutorService launchers) {
            this.client = client;
            this.request = request;
            this.measureAt = measureAt;
            this.stopAt = stopAt;
            this.metrics = metrics;
            this.finished = finished;
            this.launchers = launchers;
        }

        @Override
        public void run() {
            long sentAt = System.nanoTime();
            if (sentAt >= stopAt) {
                finished.countDown();
                return;
            }
            CompletableFuture<EchoResponse> call;
            try {
                call = client.call(EchoRpc.ECHO, request);
            } catch (RuntimeException e) {
                record(sentAt, false);
                launchers.execute(this);
                return;
            }
            call.whenCompleteAsync((response, error) -> {
                record(sentAt, error == null && request.getMessage().equals(response.getMessage()));
                launchers.execute(this);
            }, launchers);
        }

        private void record(long sentAt, boolean success) {
            if (sentAt < measureAt || sentAt >= stopAt) return;
            if (success) metrics.recordLatency(System.nanoTime() - sentAt);
            else metrics.errors.increment();
        }
    }

    private static final class Metrics {
        private final LongAdder success = new LongAdder();
        private final LongAdder errors = new LongAdder();
        private final LongAccumulator maxNanos = new LongAccumulator(Long::max, 0);
        private final AtomicLongArray latencyBuckets = new AtomicLongArray(LATENCY_BUCKETS);

        private void recordLatency(long nanos) {
            int bucket = (int) Math.min(LATENCY_BUCKETS - 1,
                    TimeUnit.NANOSECONDS.toMicros(nanos) / 100);
            latencyBuckets.incrementAndGet(bucket);
            maxNanos.accumulate(nanos);
            success.increment();
        }

        private double percentile(double fraction) {
            long count = success.sum();
            if (count == 0) return 0;
            long target = (long) Math.ceil(count * fraction);
            long seen = 0;
            for (int i = 0; i < LATENCY_BUCKETS; i++) {
                seen += latencyBuckets.get(i);
                if (seen >= target) return i / 10.0;
            }
            return (LATENCY_BUCKETS - 1) / 10.0;
        }
    }

    private record Options(String profile, String host, int port, int connections,
                           int inflightPerConnection, int payloadBytes, int warmupSeconds,
                           int durationSeconds, int serverWorkers) {
        private static Options parse(String[] args) {
            Map<String, String> values = new HashMap<>();
            for (String arg : args) {
                if (!arg.startsWith("--") || !arg.contains("=")) {
                    throw new IllegalArgumentException("Arguments must use --name=value: " + arg);
                }
                int separator = arg.indexOf('=');
                values.put(arg.substring(2, separator), arg.substring(separator + 1));
            }
            for (String name : values.keySet()) {
                if (!List.of("profile", "host", "port", "connections", "inflight", "payload",
                        "warmup", "duration", "server-workers").contains(name)) {
                    throw new IllegalArgumentException("Unknown option: " + name);
                }
            }
            String profile = values.getOrDefault("profile", "benchmark");
            if (!profile.equals("benchmark") && !profile.equals("stress")) {
                throw new IllegalArgumentException("profile must be benchmark or stress");
            }
            boolean stress = profile.equals("stress");
            int connections = number(values, "connections", stress ? 8 : 4, 1, 256);
            int inflight = number(values, "inflight", stress ? 128 : 32, 1, 4096);
            if ((long) connections * inflight > 16_384) {
                throw new IllegalArgumentException("maxInflight must not exceed 16384");
            }
            return new Options(profile, values.get("host"), number(values, "port", 9081, 1, 65535),
                    connections, inflight, number(values, "payload", stress ? 1024 : 128, 1, 1_000_000),
                    number(values, "warmup", 3, 0, 600), number(values, "duration", stress ? 30 : 10, 1, 600),
                    number(values, "server-workers", Runtime.getRuntime().availableProcessors(), 1, 1024));
        }

        private static int number(Map<String, String> values, String name, int fallback, int min, int max) {
            int value = Integer.parseInt(values.getOrDefault(name, Integer.toString(fallback)));
            if (value < min || value > max) throw new IllegalArgumentException(name + " must be " + min + ".." + max);
            return value;
        }
    }
}
