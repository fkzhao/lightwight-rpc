package dev.yumix.rpc.client;

/** Distinguishes a connection wait timeout from a response timeout. */
public final class RpcTimeoutException extends RpcException {
    public enum Stage { CONNECTION_WAIT, RESPONSE }

    private final Stage stage;

    public RpcTimeoutException(Stage stage, String message) {
        super(message, null);
        this.stage = stage;
    }

    public Stage stage() {
        return stage;
    }
}
