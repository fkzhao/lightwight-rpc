package dev.yumix.rpc.client;

/** The client's bounded queue or in-flight capacity is exhausted. */
public final class LocalOverloadException extends RpcException {
    public LocalOverloadException(String message) {
        super(message, null);
    }
}
