package io.github.fkzhao.lightweightrpc.server;

import com.google.protobuf.Message;

@FunctionalInterface
public interface RpcMethod<Req extends Message, Resp extends Message> {
    Resp handle(Req request) throws Exception;
}
