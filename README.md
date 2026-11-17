# lightweight-rpc

A small Java 17 RPC framework using Netty, a 36-byte nshead-style header, and Protocol Buffers payloads. It includes a multiplexed asynchronous client, a server with a worker pool, and an Echo example.

The Java base package and Maven group ID are `dev.yumix`.

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
mvn exec:java -Dexec.mainClass=dev.yumix.example.EchoServerMain
```

Call it from another terminal:

```bash
mvn exec:java -Dexec.mainClass=dev.yumix.example.EchoClientMain -Dexec.args="127.0.0.1 9000 hello"
```

## Wire protocol

Each TCP frame contains the following header followed by the body. All integer fields are unsigned and little-endian. The body length is limited by each endpoint's configured `maxBodyLength`.

| Offset | Size | Field | Meaning |
| ---: | ---: | --- | --- |
| 0 | 2 | `id` | Request/response correlation ID |
| 2 | 2 | `version` | Protocol version; currently `1` |
| 4 | 4 | `log_id` | Caller-supplied trace ID |
| 8 | 16 | `provider` | UTF-8 caller/server identifier, zero-padded |
| 24 | 4 | `magic_num` | `0xfb709394` |
| 28 | 4 | `method_id` | Custom RPC method ID |
| 32 | 4 | `body_len` | Body length in bytes |
| 36 | variable | `body` | Protobuf message bytes |

Requests carry the method's request protobuf directly. Responses carry the common `RpcResponse` protobuf from `rpc_common.proto`; its `payload` is the serialized method response on success, and `error_code` plus `error_message` describe failures. This project defines its own method routing and response convention; it is not a brpc compatibility implementation.

## Register a method

```java
server.register(1, EchoRequest.parser(), request ->
        EchoResponse.newBuilder().setMessage(request.getMessage()).build());
```

Use `RpcClient.request(methodId, logId, request, responseParser)` to invoke the method. The returned `CompletableFuture` allows multiple outstanding calls over one connection. The client correlates replies by the 16-bit nshead `id`; at most 65,536 requests may be in flight on a connection.

## Protocol definition

The nshead layout follows the field sizes used by the Baidu nshead header. This project's method ID and response envelope are custom conventions for this RPC framework.
