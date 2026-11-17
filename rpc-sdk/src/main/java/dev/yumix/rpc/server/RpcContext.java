package dev.yumix.rpc.server;

import java.net.SocketAddress;

/** Transport details available to an RPC method. */
public record RpcContext(long requestId, String connectionId,
                         SocketAddress localAddress, SocketAddress remoteAddress) {}
