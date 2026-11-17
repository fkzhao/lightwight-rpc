package dev.yumix.rpc.example;

import dev.yumix.rpc.example.proto.EchoRequest;
import dev.yumix.rpc.example.proto.EchoResponse;
import dev.yumix.rpc.server.RpcContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class EchoService {
    private static final Logger LOG = LoggerFactory.getLogger(EchoService.class);

    private EchoService() {}

    static EchoResponse handle(EchoRequest request, RpcContext context) throws InterruptedException {
        long delayMs = Integer.toUnsignedLong(request.getDelayMs());
        if (delayMs > 10_000) throw new IllegalArgumentException("delay_ms must be at most 10000");
        LOG.info("Echo handling message={} delayMs={} connection={} requestId={} remote={}",
                request.getMessage(), delayMs, context.connectionId(),
                Long.toUnsignedString(context.requestId()), context.remoteAddress());
        if (delayMs > 0) Thread.sleep(delayMs);
        return EchoResponse.newBuilder().setMessage(request.getMessage()).build();
    }
}
