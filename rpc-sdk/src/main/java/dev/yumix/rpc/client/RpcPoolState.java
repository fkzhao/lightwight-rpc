package dev.yumix.rpc.client;

/** Aggregate state of the connections managed by an RPC client pool. */
public enum RpcPoolState {
    STARTING, READY, DEGRADED, RECONNECTING, UNAVAILABLE, CLOSED
}
