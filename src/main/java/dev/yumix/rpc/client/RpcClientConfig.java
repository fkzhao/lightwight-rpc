package dev.yumix.rpc.client;

/** Timeouts and size limits for a persistent RPC connection. */
public record RpcClientConfig(long requestTimeoutMillis, int maxBodyLength,
                              int connectTimeoutMillis, long heartbeatIdleMillis,
                              long heartbeatTimeoutMillis, long reconnectInitialMillis,
                              long reconnectMaxMillis) {
    public RpcClientConfig {
        if (requestTimeoutMillis < 1) throw new IllegalArgumentException("requestTimeoutMillis must be positive");
        if (maxBodyLength < 0) throw new IllegalArgumentException("maxBodyLength must be non-negative");
        if (connectTimeoutMillis < 1) throw new IllegalArgumentException("connectTimeoutMillis must be positive");
        if (heartbeatIdleMillis < 1) throw new IllegalArgumentException("heartbeatIdleMillis must be positive");
        if (heartbeatTimeoutMillis < 1) throw new IllegalArgumentException("heartbeatTimeoutMillis must be positive");
        if (reconnectInitialMillis < 1 || reconnectMaxMillis < reconnectInitialMillis) {
            throw new IllegalArgumentException("Invalid reconnect interval");
        }
    }

    public static RpcClientConfig defaults(long requestTimeoutMillis, int maxBodyLength) {
        return new RpcClientConfig(requestTimeoutMillis, maxBodyLength,
                3_000, 30_000, 10_000, 500, 30_000);
    }
}
