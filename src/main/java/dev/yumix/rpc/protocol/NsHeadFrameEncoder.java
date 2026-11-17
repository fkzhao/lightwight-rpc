package dev.yumix.rpc.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.nio.charset.StandardCharsets;

public final class NsHeadFrameEncoder extends MessageToByteEncoder<NsHeadFrame> {
    @Override
    protected void encode(ChannelHandlerContext ctx, NsHeadFrame frame, ByteBuf out) {
        byte[] provider = frame.provider().getBytes(StandardCharsets.UTF_8);
        out.writeShortLE(frame.id());
        out.writeShortLE(frame.version());
        out.writeIntLE((int) frame.logId());
        out.writeBytes(provider);
        out.writeZero(16 - provider.length);
        out.writeIntLE(NsHeadFrame.MAGIC);
        out.writeIntLE((int) frame.methodId());
        out.writeIntLE(frame.body().length);
        out.writeBytes(frame.body());
    }
}
