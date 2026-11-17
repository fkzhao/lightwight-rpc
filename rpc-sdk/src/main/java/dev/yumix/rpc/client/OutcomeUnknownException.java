package dev.yumix.rpc.client;

/** A written request lost its connection before a response established its outcome. */
public final class OutcomeUnknownException extends RpcException {
    public OutcomeUnknownException(String message) {
        super(message, null);
    }
}
