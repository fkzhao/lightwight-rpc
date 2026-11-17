package dev.yumix.rpc.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.DecoderException;

import java.nio.charset.StandardCharsets;
import java.util.List;

/** Splits a TCP byte stream into complete 44-byte v3 headers and payloads. */
public final class NsHeadFrameDecoder extends ByteToMessageDecoder {
    private final int maxBodyLength;

    public NsHeadFrameDecoder(int maxBodyLength) {
        if (maxBodyLength < 0) throw new IllegalArgumentException("maxBodyLength must be non-negative");
        this.maxBodyLength = maxBodyLength;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (in.readableBytes() < NsHeadFrame.BASE_HEADER_LENGTH) return;
        int start = in.readerIndex();
        int magic = in.getIntLE(start + 24);
        if (magic != NsHeadFrame.MAGIC) throw new DecoderException("Invalid nshead magic: 0x" + Integer.toHexString(magic));
        int version = in.getUnsignedShortLE(start + 2);
        if (version != NsHeadFrame.PROTOCOL_VERSION) {
            throw new DecoderException("Unsupported nshead version: " + version);
        }
        long unsignedLength = in.getUnsignedIntLE(start + 32);
        if (unsignedLength > maxBodyLength || unsignedLength > Integer.MAX_VALUE - NsHeadFrame.HEADER_LENGTH) {
            throw new DecoderException("nshead body length " + unsignedLength + " exceeds limit " + maxBodyLength);
        }
        if (in.readableBytes() < NsHeadFrame.HEADER_LENGTH) return;
        int bodyLength = (int) unsignedLength;
        if (in.readableBytes() < NsHeadFrame.HEADER_LENGTH + bodyLength) return;

        int id = in.getUnsignedShortLE(start);
        long logId = in.getUnsignedIntLE(start + 4);
        byte[] providerBytes = new byte[16];
        in.getBytes(start + 8, providerBytes);
        String provider = decodeProvider(providerBytes);
        long methodId = in.getUnsignedIntLE(start + 28);
        long requestId = in.getLongLE(start + 36);
        if (requestId == 0 || id != ((int) requestId & 0xffff)) {
            throw new DecoderException("Invalid nshead request ID");
        }
        in.skipBytes(NsHeadFrame.HEADER_LENGTH);
        byte[] body = new byte[bodyLength];
        in.readBytes(body);
        out.add(new NsHeadFrame(id, version, logId, provider, methodId, requestId, body));
    }

    private static String decodeProvider(byte[] value) {
        int length = 0;
        while (length < value.length && value[length] != 0) length++;
        return new String(value, 0, length, StandardCharsets.UTF_8);
    }
}
