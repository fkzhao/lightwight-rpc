package dev.yumix.rpc.client;

import java.util.List;

/** Immutable snapshot of client pool connections and counters. */
public record RpcPoolStats(RpcPoolState state, int readyConnections, int queuedCalls,
                           long queuedBytes, int inFlightCalls, int consecutiveReconnectFailures,
                           long reconnectAttempts, long heartbeatTimeouts,
                           long connectionWaitTimeouts, long responseTimeouts,
                           long discardedLateResponses, List<Connection> connections) {
    public RpcPoolStats {
        connections = List.copyOf(connections);
    }

    public record Connection(String id, String localAddress, String remoteAddress, int inFlight) {}
}
