# Local server benchmark results

Run date: 2026-11-17 (Asia/Shanghai). These are single-run local measurements, not production capacity guarantees.

The measurements in this document before the SDK pool comparison use the earlier `RpcClient` transport. Use `--client=legacy` to reproduce them. The load generator now defaults to `RpcClientPool`.

## SDK pool comparison

Run date: 2026-11-17 (Asia/Shanghai). The pool and legacy clients used the same separate benchmark server process on loopback, 8 server workers, OpenJDK 17.0.20.1, and the profiles below. Each row is one run; results may vary between runs.

| Profile | Client | Connections | In flight per connection | Payload | Warmup | Measurement | Success | Errors | QPS | p50 | p95 | p99 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| benchmark | pool | 4 | 32 | 128 bytes | 2 s | 5 s | 526,541 | 0 | 105,308.2 | 1.20 ms | 1.30 ms | 1.40 ms |
| benchmark | legacy | 4 | 32 | 128 bytes | 2 s | 5 s | 494,337 | 0 | 98,867.4 | 1.20 ms | 1.40 ms | 1.50 ms |
| stress | pool | 8 | 128 | 1,024 bytes | 2 s | 10 s | 1,032,167 | 0 | 103,216.7 | 9.90 ms | 10.70 ms | 11.60 ms |
| stress | legacy | 8 | 128 | 1,024 bytes | 2 s | 10 s | 941,960 | 0 | 94,196.0 | 10.80 ms | 11.50 ms | 12.00 ms |

The SDK pool showed no throughput or p99 regression in these same-process comparisons. These measurements include client scheduling, loopback TCP, protobuf serialization, and server work; they do not isolate server-only capacity.

## Environment and method

- Java: OpenJDK 17.0.20.1
- Available processors reported by Java: 14
- Client and server: separate JVM processes on the same macOS host, communicating over loopback TCP
- Server: `BenchmarkServerMain`, 8 worker threads, per-request INFO logging disabled
- Client: `LoadTestMain`, fixed closed-loop concurrency, 5-second RPC timeout
- Payload: Echo protobuf message containing an ASCII string of the stated byte length
- Latency: client-observed end-to-end time, recorded in 0.1 ms buckets
- Throughput: successful measured calls divided by the configured measurement duration

| Profile | Connections | In flight per connection | Payload | Warmup | Measurement | Success | Errors | Throughput | p50 | p95 | p99 | Max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| benchmark | 4 | 32 | 128 bytes | 2 s | 5 s | 466,579 | 0 | 93,315.8 req/s | 1.30 ms | 1.50 ms | 1.60 ms | 4.59 ms |
| stress | 8 | 128 | 1,024 bytes | 2 s | 10 s | 915,570 | 0 | 91,557.0 req/s | 11.10 ms | 11.70 ms | 12.10 ms | 28.22 ms |

## Reproduce

Build first with `mvn clean install`. Start the benchmark server:

```bash
mvn -q -f rpc-benchmark/pom.xml exec:java -Dexec.mainClass=dev.yumix.rpc.benchmark.BenchmarkServerMain -Dexec.args="9081 8"
```

Run the two profiles in another terminal:

```bash
mvn -q -f rpc-benchmark/pom.xml exec:java -Dexec.mainClass=dev.yumix.rpc.benchmark.LoadTestMain -Dexec.args="--client=legacy --profile=benchmark --host=127.0.0.1 --port=9081 --connections=4 --inflight=32 --warmup=2 --duration=5 --payload=128"
mvn -q -f rpc-benchmark/pom.xml exec:java -Dexec.mainClass=dev.yumix.rpc.benchmark.LoadTestMain -Dexec.args="--client=legacy --profile=stress --host=127.0.0.1 --port=9081 --connections=8 --inflight=128 --warmup=2 --duration=10 --payload=1024"
```

The results include client scheduling, loopback networking, protobuf serialization, and server execution. Repeat runs under the same settings to compare changes. For isolated server capacity, run the load generator on a separate host and monitor server CPU, memory, and network use.

## Local maximum QPS sweep

Run date: 2026-11-17 (Asia/Shanghai). The host is an Apple M4 Pro Mac with 14 CPU cores and 24 GiB of memory, running macOS 27.2.1 and OpenJDK 17.0.20.1. The server and load generator ran in separate JVM processes on the same host over loopback TCP. Per-request logging was disabled. Each call echoed a one-byte ASCII message; the nshead header, protobuf encoding, and response envelope add wire bytes. All measured runs below had zero RPC errors.

The initial sweep used 8 server worker threads, a 2-second warmup, and a 6-second measurement window. These rows show how total in-flight requests affect throughput and p99 latency:

| Client connections | In flight per connection | Total in flight | QPS | p99 |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 1 | 15,961.7 | 0.10 ms |
| 1 | 8 | 8 | 59,231.8 | 0.20 ms |
| 2 | 16 | 32 | 87,781.2 | 0.60 ms |
| 4 | 32 | 128 | 93,689.2 | 1.60 ms |
| 8 | 16 | 128 | 95,798.7 | 1.50 ms |
| 8 | 64 | 512 | 97,423.3 | 5.80 ms |
| 16 | 16 | 256 | 96,114.3 | 3.00 ms |
| 16 | 64 | 1,024 | 94,882.0 | 12.10 ms |

With 14 server workers, the nearby settings were measured for 10 seconds after a 3-second warmup:

| Client connections | In flight per connection | Total in flight | QPS | p99 |
| ---: | ---: | ---: | ---: | ---: |
| 8 | 32 | 256 | 99,401.8 | 3.00 ms |
| 8 | 64 | 512 | 99,633.3 | 6.30 ms |
| 8 | 128 | 1,024 | **100,694.5** | 11.10 ms |
| 8 | 256 | 2,048 | 99,425.5 | 21.60 ms |
| 8 | 512 | 4,096 | 96,561.5 | 45.20 ms |

At 8 connections and 64 in-flight calls per connection, 4 server workers produced 98,249.8 QPS in a 10-second run. Two simultaneous load-generator JVMs produced 98,079.9 aggregate QPS with 8 server workers and 97,701.9 with 14 server workers; four JVMs produced 96,271.3 aggregate QPS with 8 workers. Each generator used 4 connections with 32 in-flight calls per connection, a 3-second warmup, and a 10-second measurement window. Adding generator processes did not increase the observed throughput beyond the single-process peak.

Longer validation with 14 server workers, a 5-second warmup, and a 30-second measurement window:

| Client connections | In flight per connection | Successful calls | Errors | QPS | p99 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 64 | 3,011,820 | 0 | **100,394.0** | 5.70 ms |
| 8 | 64 | 3,006,269 | 0 | 100,209.0 | 5.70 ms |
| 8 | 128 | 3,008,042 | 0 | 100,268.1 | 10.80 ms |

The highest observed short run was 100,694.5 QPS. The repeated 30-second runs support approximately 100,000 QPS as the stable same-host result for this one-byte Echo workload. More in-flight calls increased latency without sustained throughput gains. This is an end-to-end limit for this server and load generator sharing one Mac; it does not isolate server-only capacity or predict performance over a real network.

To reproduce the 30-second run from the repository root after `mvn clean install`, build the benchmark classpath:

```bash
mvn -q -f rpc-benchmark/pom.xml dependency:build-classpath -Dmdep.outputFile=/tmp/rpc-benchmark-classpath.txt
read -r RPC_BENCH_CP < /tmp/rpc-benchmark-classpath.txt
```

Start the server in one terminal:

```bash
java -Dorg.slf4j.simpleLogger.defaultLogLevel=error -cp "${RPC_BENCH_CP}:rpc-benchmark/target/classes" dev.yumix.rpc.benchmark.BenchmarkServerMain 9081 14
```

Run the client in another terminal, after setting `RPC_BENCH_CP` there as well:

```bash
read -r RPC_BENCH_CP < /tmp/rpc-benchmark-classpath.txt
java -Dorg.slf4j.simpleLogger.defaultLogLevel=error -cp "${RPC_BENCH_CP}:rpc-benchmark/target/classes" dev.yumix.rpc.benchmark.LoadTestMain --client=legacy --profile=benchmark --host=127.0.0.1 --port=9081 --connections=8 --inflight=64 --payload=1 --warmup=5 --duration=30
```
