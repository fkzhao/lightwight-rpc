package dev.yumix.rpc;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import dev.yumix.rpc.protocol.NsHeadFrame;

import java.util.Objects;

/** Shared method metadata used by an RPC client and server. */
public record RpcMethodDescriptor<Req extends Message, Resp extends Message>(
        long methodId, Parser<Req> requestParser, Parser<Resp> responseParser) {
    public RpcMethodDescriptor {
        if (methodId <= NsHeadFrame.PING_METHOD_ID || methodId >= NsHeadFrame.PONG_METHOD_ID) {
            throw new IllegalArgumentException("methodId is reserved for heartbeat control frames");
        }
        Objects.requireNonNull(requestParser, "requestParser");
        Objects.requireNonNull(responseParser, "responseParser");
    }
}
