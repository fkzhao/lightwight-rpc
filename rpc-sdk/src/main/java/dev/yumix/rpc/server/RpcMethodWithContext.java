package dev.yumix.rpc.server;

import com.google.protobuf.Message;

@FunctionalInterface
public interface RpcMethodWithContext<Req extends Message, Resp extends Message> {
    Resp handle(Req request, RpcContext context) throws Exception;
}
