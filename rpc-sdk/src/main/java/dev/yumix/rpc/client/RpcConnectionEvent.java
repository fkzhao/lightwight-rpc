package dev.yumix.rpc.client;

/** Connection or pool state transition delivered to an application listener. */
public record RpcConnectionEvent(RpcPoolState state, String connectionId,
                                 String localAddress, String remoteAddress, String reason) {}
