# Local server benchmark results

Run date: 2026-11-17 (Asia/Shanghai). These are single-run local measurements, not production capacity guarantees.

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
mvn -q -f rpc-benchmark/pom.xml exec:java -Dexec.mainClass=dev.yumix.rpc.benchmark.LoadTestMain -Dexec.args="--profile=benchmark --host=127.0.0.1 --port=9081 --connections=4 --inflight=32 --warmup=2 --duration=5 --payload=128"
mvn -q -f rpc-benchmark/pom.xml exec:java -Dexec.mainClass=dev.yumix.rpc.benchmark.LoadTestMain -Dexec.args="--profile=stress --host=127.0.0.1 --port=9081 --connections=8 --inflight=128 --warmup=2 --duration=10 --payload=1024"
```

The results include client scheduling, loopback networking, protobuf serialization, and server execution. Repeat runs under the same settings to compare changes. For isolated server capacity, run the load generator on a separate host and monitor server CPU, memory, and network use.
