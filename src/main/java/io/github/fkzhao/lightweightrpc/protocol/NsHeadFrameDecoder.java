package io.github.fkzhao.lightweightrpc.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.DecoderException;

import java.nio.charset.StandardCharsets;
import java.util.List;

/** Splits a TCP byte stream into complete 36-byte nshead headers and payloads. */
public final class NsHeadFrameDecoder extends ByteToMessageDecoder {
    private final int maxBodyLength;

    public NsHeadFrameDecoder(int maxBodyLength) {
        if (maxBodyLength < 0) throw new IllegalArgumentException("maxBodyLength must be non-negative");
        this.maxBodyLength = maxBodyLength;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (in.readableBytes() < NsHeadFrame.HEADER_LENGTH) return;
        int start = in.readerIndex();
        int magic = in.getIntLE(start + 24);
        if (magic != NsHeadFrame.MAGIC) throw new DecoderException("Invalid nshead magic: 0x" + Integer.toHexString(magic));
        long unsignedLength = in.getUnsignedIntLE(start + 32);
        if (unsignedLength > maxBodyLength) {
            throw new DecoderException("nshead body length " + unsignedLength + " exceeds limit " + maxBodyLength);
        }
        int bodyLength = (int) unsignedLength;
        if (in.readableBytes() < NsHeadFrame.HEADER_LENGTH + bodyLength) return;

        int id = in.getUnsignedShortLE(start);
        int version = in.getUnsignedShortLE(start + 2);
        long logId = in.getUnsignedIntLE(start + 4);
        byte[] providerBytes = new byte[16];
        in.getBytes(start + 8, providerBytes);
        String provider = decodeProvider(providerBytes);
        long methodId = in.getUnsignedIntLE(start + 28);
        in.skipBytes(NsHeadFrame.HEADER_LENGTH);
        byte[] body = new byte[bodyLength];
        in.readBytes(body);
        out.add(new NsHeadFrame(id, version, logId, provider, methodId, body));
    }

    private static String decodeProvider(byte[] value) {
        int length = 0;
        while (length < value.length && value[length] != 0) length++;
        return new String(value, 0, length, StandardCharsets.UTF_8);
    }
}
