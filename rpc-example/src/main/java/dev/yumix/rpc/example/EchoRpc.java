package dev.yumix.rpc.example;

import dev.yumix.rpc.RpcMethodDescriptor;
import dev.yumix.rpc.example.proto.EchoRequest;
import dev.yumix.rpc.example.proto.EchoResponse;

public final class EchoRpc {
    public static final long ECHO_ID = 1;
    public static final RpcMethodDescriptor<EchoRequest, EchoResponse> ECHO =
            new RpcMethodDescriptor<>(ECHO_ID, EchoRequest.parser(), EchoResponse.parser());

    private EchoRpc() {}
}
