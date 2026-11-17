# lightweight-rpc

A small Java 17 RPC framework using Netty, a 36-byte nshead-style header, and Protocol Buffers payloads. It includes a multiplexed asynchronous client, a server with a worker pool, and an Echo example.

The Java base package and Maven group ID are `dev.yumix.rpc`.

## Requirements

- JDK 17
- Maven 3.8+

## Build

```bash
mvn compile
```

Protocol Buffer Java sources are generated from `src/main/proto` during the Maven build.

## Run the Echo example

Start the server:

```bash
mvn exec:java -Dexec.mainClass=dev.yumix.rpc.example.EchoServerMain
```

Call it from another terminal:

```bash
mvn exec:java -Dexec.mainClass=dev.yumix.rpc.example.EchoClientMain -Dexec.args="127.0.0.1 9080 hello"
```

## Observe multiplexing on one TCP connection

To see server logs separately, start the server in terminal 1:

```bash
mvn compile exec:java -Dexec.mainClass=dev.yumix.rpc.example.EchoServerMain
```

Run the multiplexing demo in terminal 2. With no arguments it connects to the running Echo server at `127.0.0.1:9080`:

```bash
mvn -q compile exec:java -Dexec.mainClass=dev.yumix.rpc.example.MultiplexingDemoMain
```

The server logs connection open/close, each request ID, method ID, connection ID, remote address, and completion time. Its Echo handler also logs the request label and delay. All four requests have the same server connection ID and remote port; that remote port matches the client's local port. The completion order differs from the send order.

For both sides in one terminal, explicitly start the embedded server:

```bash
mvn compile exec:java -Dexec.mainClass=dev.yumix.rpc.example.MultiplexingDemoMain -Dexec.args="--standalone"
```

The standalone mode starts its own server on a random loopback port. Its requests do not reach a separately running `EchoServerMain`.
Run the server or standalone demo without Maven's `-q` option so its INFO logs remain visible.

## Wire protocol

Each TCP frame contains the following header followed by the body. All integer fields are unsigned and little-endian. The body length is limited by each endpoint's configured `maxBodyLength`.

| Offset | Size | Field | Meaning |
| ---: | ---: | --- | --- |
| 0 | 2 | `id` | Low 16 bits of the request ID |
| 2 | 2 | `version` | Protocol version; currently `2` |
| 4 | 4 | `log_id` | Caller-supplied trace ID |
| 8 | 16 | `provider` | UTF-8 caller/server identifier, zero-padded |
| 24 | 4 | `magic_num` | `0xfb709394` |
| 28 | 4 | `method_id` | Custom RPC method ID |
| 32 | 4 | `body_len` | Body length in bytes |
| 36 | variable | `body` | Protobuf envelope bytes |

Requests carry `RpcRequest` from `rpc_common.proto`, with a 64-bit `request_id` and a `payload` containing the serialized method request. Responses carry `RpcResponse` with the same `request_id`; its `payload` is the serialized method response on success, and `error_code` plus `error_message` describe failures. The 36-byte nshead header is unchanged. Protocol version 2 is required on both endpoints; version 1 peers are not compatible with this envelope. This project defines its own method routing and response convention; it is not a brpc compatibility implementation.

## Register a method

```java
server.register(1, EchoRequest.parser(), request ->
        EchoResponse.newBuilder().setMessage(request.getMessage()).build());
```

Use `RpcClient.request(methodId, logId, request, responseParser)` to invoke the method. The returned `CompletableFuture` allows multiple outstanding calls over one connection. The client correlates replies by the 64-bit `request_id` in the protobuf envelope, so responses may arrive out of order. The nshead `log_id` remains a caller-supplied trace ID.

## Protocol definition

The nshead layout follows the field sizes used by the Baidu nshead header. This project's method ID and response envelope are custom conventions for this RPC framework.
