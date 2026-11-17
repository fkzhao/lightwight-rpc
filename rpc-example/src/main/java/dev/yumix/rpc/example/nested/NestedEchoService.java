package dev.yumix.rpc.example.nested;

import dev.yumix.rpc.example.EchoRpc;
import dev.yumix.rpc.example.proto.EchoRequest;
import dev.yumix.rpc.example.proto.EchoResponse;
import dev.yumix.rpc.server.RpcContext;
import dev.yumix.rpc.server.RpcRoute;
import dev.yumix.rpc.server.RpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Demonstrates discovery of a service below the scan anchor's package. */
@RpcService
public final class NestedEchoService {
    private static final Logger LOG = LoggerFactory.getLogger(NestedEchoService.class);

    @RpcRoute(methodId = EchoRpc.NESTED_ECHO_ID)
    public EchoResponse handle(EchoRequest request, RpcContext context) {
        LOG.info("Nested Echo handling message={} connection={} requestId={}",
                request.getMessage(), context.connectionId(),
                Long.toUnsignedString(context.requestId()));
        return EchoResponse.newBuilder().setMessage("nested:" + request.getMessage()).build();
    }
}
