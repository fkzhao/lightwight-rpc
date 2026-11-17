package dev.yumix.rpc.client;

public class RpcException extends RuntimeException {
    private final long errorCode;

    public RpcException(long errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public RpcException(String message, Throwable cause) {
        super(message, cause);
        this.errorCode = 0;
    }

    public long errorCode() { return errorCode; }
}
