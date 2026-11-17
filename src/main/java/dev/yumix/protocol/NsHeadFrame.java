package dev.yumix.protocol;

import java.util.Objects;

/** A decoded nshead header and its protobuf payload. */
public record NsHeadFrame(int id, int version, long logId, String provider, long methodId, byte[] body) {
    public static final int HEADER_LENGTH = 36;
    public static final int MAGIC = 0xfb709394;
    public static final int PROTOCOL_VERSION = 1;

    public NsHeadFrame {
        if ((id & ~0xffff) != 0) throw new IllegalArgumentException("id must fit in uint16");
        if ((version & ~0xffff) != 0) throw new IllegalArgumentException("version must fit in uint16");
        if ((logId & ~0xffff_ffffL) != 0) throw new IllegalArgumentException("logId must fit in uint32");
        if ((methodId & ~0xffff_ffffL) != 0) throw new IllegalArgumentException("methodId must fit in uint32");
        Objects.requireNonNull(provider, "provider");
        Objects.requireNonNull(body, "body");
        if (provider.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 16) {
            throw new IllegalArgumentException("provider must be at most 16 UTF-8 bytes");
        }
    }
}
