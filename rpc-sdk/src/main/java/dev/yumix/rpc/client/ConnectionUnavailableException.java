package dev.yumix.rpc.client;

/** No connection became ready within the configured reconnect failure budget. */
public final class ConnectionUnavailableException extends RpcException {
    public ConnectionUnavailableException(String message) {
        super(message, null);
    }
}
