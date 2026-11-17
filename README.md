# lightweight-rpc

A small Java 17 RPC framework using Netty, a 44-byte extended nshead header, and Protocol Buffers payloads. It includes a multiplexed asynchronous client, a server with a worker pool, and an Echo example.

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

Each TCP frame contains the 36-byte nshead base, an 8-byte request ID extension, and then the body. All header integer fields are little-endian. The body length excludes the 44-byte header and is limited by each endpoint's configured `maxBodyLength`.

| Offset | Size | Field | Meaning |
| ---: | ---: | --- | --- |
| 0 | 2 | `id` | Low 16 bits of the request ID |
| 2 | 2 | `version` | Protocol version; currently `3` |
| 4 | 4 | `log_id` | Caller-supplied trace ID |
| 8 | 16 | `provider` | UTF-8 caller/server identifier, zero-padded |
| 24 | 4 | `magic_num` | `0xfb709394` |
| 28 | 4 | `method_id` | Custom RPC method ID |
| 32 | 4 | `body_len` | Body length in bytes |
| 36 | 8 | `request_id` | Full 64-bit request/response correlation ID |
| 44 | variable | `body` | Protobuf message bytes |

Requests carry the serialized method request directly in the body. Responses carry `RpcResponse` from `rpc_common.proto`; its `payload` is the serialized method response on success, and `error_code` plus `error_message` describe failures. The server echoes the request's header `request_id` in the response header. Version 3 is required on both endpoints and is not wire-compatible with version 2. This project defines its own method routing and response convention; it is not a brpc compatibility implementation.

## Register a method

```java
server.register(1, EchoRequest.parser(), request ->
        EchoResponse.newBuilder().setMessage(request.getMessage()).build());
```

Use `RpcClient.request(methodId, logId, request, responseParser)` to invoke the method. The returned `CompletableFuture` allows multiple outstanding calls over one connection. The client correlates replies by the 64-bit `request_id` in the response header before parsing the response protobuf, so responses may arrive out of order. The nshead `log_id` remains a caller-supplied trace ID.

## Protocol definition

The first 36 bytes follow the field sizes used by the Baidu nshead header. The 8-byte request ID extension, method ID, and response envelope are custom conventions for this RPC framework.
