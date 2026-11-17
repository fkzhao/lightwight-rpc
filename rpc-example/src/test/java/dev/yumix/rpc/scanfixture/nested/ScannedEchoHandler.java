package dev.yumix.rpc.scanfixture.nested;

import dev.yumix.rpc.example.proto.EchoRequest;
import dev.yumix.rpc.example.proto.EchoResponse;
import dev.yumix.rpc.server.RpcRoute;
import dev.yumix.rpc.server.RpcService;

@RpcService
public final class ScannedEchoHandler {
    @RpcRoute(methodId = 703)
    public EchoResponse echo(EchoRequest request) {
        return EchoResponse.newBuilder().setMessage("scanned:" + request.getMessage()).build();
    }
}
