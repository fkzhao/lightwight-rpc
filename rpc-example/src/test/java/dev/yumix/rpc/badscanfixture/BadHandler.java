package dev.yumix.rpc.badscanfixture;

import dev.yumix.rpc.server.RpcRoute;
import dev.yumix.rpc.server.RpcService;

@RpcService
public final class BadHandler {
    @RpcRoute(methodId = 705)
    public String invalid(String request) {
        return request;
    }
}
