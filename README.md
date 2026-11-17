# lightweight-rpc

Java 17 SDK for multiplexed RPC over a persistent TCP connection. The wire format is a 44-byte extended nshead v3 header followed by a Protocol Buffers body.

## Modules

| Module | Purpose |
| --- | --- |
| `rpc-sdk` | Reusable client, server, framing, heartbeat, and reconnect logic |
| `rpc-example` | Echo protobuf contract and runnable integration examples |
| `rpc-benchmark` | Server load generator and dedicated benchmark server |

Build and install all modules with JDK 17 and Maven 3.8+:

```bash
mvn clean install
```

Each module generates Java sources from its own `src/main/proto` files. The reusable JAR is `rpc-sdk/target/rpc-sdk-0.1.0-SNAPSHOT.jar`.

## Use the SDK from another Maven project

```xml
<dependency>
  <groupId>dev.yumix.rpc</groupId>
  <artifactId>rpc-sdk</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

Define the business request and response in your own `.proto` file and generate their Java classes during your Maven build. The example contract is in `rpc-example/src/main/proto/echo.proto`. Use the same method ID and protobuf message types on both sides:

```java
RpcMethodDescriptor<EchoRequest, EchoResponse> echo =
        new RpcMethodDescriptor<>(1, EchoRequest.parser(), EchoResponse.parser());

RpcServer server = new RpcServer(new InetSocketAddress("0.0.0.0", 9080),
        "my-server", 8, 4 * 1024 * 1024);
server.register(echo, request ->
        EchoResponse.newBuilder().setMessage(request.getMessage()).build());
server.start();

try (RpcClient client = RpcClient.connect(new InetSocketAddress("127.0.0.1", 9080),
        "my-app", RpcClientConfig.defaults(3_000, 4 * 1024 * 1024))) {
    EchoResponse reply = client.call(echo,
            EchoRequest.newBuilder().setMessage("hello").build()).get();
    System.out.println(reply.getMessage());
}
```

Keep one `RpcClient` per endpoint and reuse it across calls. `call(...)` is asynchronous; the SDK correlates out-of-order responses by the 64-bit header request ID. Close the client and server during application shutdown. Use `RpcClient.open(...)` when the server may be unavailable at startup; calls fail fast until `isConnected()` becomes true.

The client caps pending calls at 4,096 per connection by default. Supply the eight-argument `RpcClientConfig` constructor to change `maxInFlight`; calls above the limit fail immediately with `RpcException`. The server uses a bounded worker queue of 4,096 tasks by default. Use the seven-argument `RpcServer` constructor to set `workerQueueCapacity`. When the queue is full, the server returns error code `4` (`RPC server overloaded`) without closing the connection. These bounds prevent slow handlers from growing the request backlog indefinitely.

`rpc-sdk` depends on `slf4j-api` but does not force a logging provider. An application can supply its own SLF4J provider.

## Run the Echo example

After `mvn install`, start the server in terminal 1:

```bash
mvn -f rpc-example/pom.xml exec:java -Dexec.mainClass=dev.yumix.rpc.example.EchoServerMain
```

Run one call in terminal 2:

```bash
mvn -q -f rpc-example/pom.xml exec:java -Dexec.mainClass=dev.yumix.rpc.example.EchoClientMain
```

Run the multiplexing client to see four concurrent calls on one connection:

```bash
mvn -q -f rpc-example/pom.xml exec:java -Dexec.mainClass=dev.yumix.rpc.example.MultiplexingDemoMain
```

`MultiplexingDemoMain --standalone` starts its own temporary server instead. The Echo server logs the connection ID, request ID, and completion time for each call. Use the command without Maven's `-q` option on the server so INFO logs remain visible.

To observe automatic reconnection, keep this client running while stopping and restarting the Echo server:

```bash
mvn -f rpc-example/pom.xml exec:java -Dexec.mainClass=dev.yumix.rpc.example.LongConnectionClientMain
```

## Long connection defaults

| Setting | Default |
| --- | ---: |
| TCP connect timeout | 3 seconds |
| Client read idle before PING | 30 seconds |
| PONG timeout | 10 seconds |
| Server read idle before closing | 90 seconds |
| Reconnect delay | 500 ms initially, exponential up to 30 seconds, with jitter |
| RPC request timeout | Supplied when constructing the client |
| Maximum pending calls per client connection | 4,096 |
| Maximum queued server worker tasks | 4,096 |

PING uses `method_id=0`; PONG uses `method_id=0xFFFFFFFF`. Both have an empty body and use the v3 header request ID. The server responds on the network thread. Calls interrupted by a disconnect fail and are never replayed automatically because the server may already have processed them. New calls fail fast while disconnected. `RpcClientConfig` configures the client timeouts; the optional fifth `RpcServer` constructor argument sets its read idle timeout in milliseconds.

## Server pressure and benchmark tests

The benchmark module has a bounded-concurrency load generator. It reports successful calls per second, errors, and p50/p95/p99 latency. For a separate server process, start:

```bash
mvn -f rpc-benchmark/pom.xml exec:java -Dexec.mainClass=dev.yumix.rpc.benchmark.BenchmarkServerMain
```

Then run the benchmark profile in another terminal:

```bash
mvn -q -f rpc-benchmark/pom.xml exec:java -Dexec.mainClass=dev.yumix.rpc.benchmark.LoadTestMain -Dexec.args="--profile=benchmark --host=127.0.0.1 --port=9081"
```

Run a longer stress profile:

```bash
mvn -q -f rpc-benchmark/pom.xml exec:java -Dexec.mainClass=dev.yumix.rpc.benchmark.LoadTestMain -Dexec.args="--profile=stress --host=127.0.0.1 --port=9081"
```

Options include `--connections`, `--inflight`, `--payload`, `--warmup`, and `--duration`. Omitting `--host` starts an embedded server in the same JVM for a quick local baseline. The separate-process mode is more representative. The test uses closed-loop fixed concurrency; reported latency includes the client, network, and server. Percentiles use 0.1 ms buckets. Run on a stable machine and compare results only under the same settings.

See [`rpc-benchmark/RESULTS.md`](rpc-benchmark/RESULTS.md) for a measured local run and exact reproduction settings.

## Wire protocol

The first 36 bytes follow the nshead field sizes. This project extends that base with an 8-byte request ID. All header integers are little-endian; `body_len` excludes the 44-byte header.

| Offset | Size | Field | Meaning |
| ---: | ---: | --- | --- |
| 0 | 2 | `id` | Low 16 bits of the request ID |
| 2 | 2 | `version` | Protocol version `3` |
| 4 | 4 | `log_id` | Caller-supplied trace ID |
| 8 | 16 | `provider` | UTF-8 identifier, zero-padded |
| 24 | 4 | `magic_num` | `0xfb709394` |
| 28 | 4 | `method_id` | RPC method or heartbeat control ID |
| 32 | 4 | `body_len` | Body length in bytes |
| 36 | 8 | `request_id` | Full 64-bit correlation ID |
| 44 | variable | `body` | Protobuf message bytes |

Requests carry their method's protobuf directly. Responses carry `RpcResponse` from `rpc_common.proto`, with either a serialized method response or an error. The server echoes the header request ID. Version 3 is not wire-compatible with version 2 or brpc.
