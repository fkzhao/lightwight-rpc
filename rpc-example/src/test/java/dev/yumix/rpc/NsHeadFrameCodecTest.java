package dev.yumix.rpc;

import dev.yumix.rpc.example.proto.EchoRequest;
import dev.yumix.rpc.protocol.NsHeadFrame;
import dev.yumix.rpc.protocol.NsHeadFrameDecoder;
import dev.yumix.rpc.protocol.NsHeadFrameEncoder;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class NsHeadFrameCodecTest {
    @Test
    void requestIdLivesInExtendedHeaderAndBodyIsRawProtobuf() throws Exception {
        long requestId = 0x0123_4567_89ab_cdefL;
        EchoRequest request = EchoRequest.newBuilder().setMessage("header-id").build();
        NsHeadFrame frame = new NsHeadFrame(0xcdef, NsHeadFrame.PROTOCOL_VERSION,
                7, "demo-client", 1, requestId, request.toByteArray());

        EmbeddedChannel encoder = new EmbeddedChannel(new NsHeadFrameEncoder());
        EmbeddedChannel decoder = new EmbeddedChannel(new NsHeadFrameDecoder(1024));
        try {
            assertTrue(encoder.writeOutbound(frame));
            ByteBuf wire = encoder.readOutbound();
            try {
                assertEquals(44 + request.getSerializedSize(), wire.readableBytes());
                assertEquals(0xcdef, wire.getUnsignedShortLE(0));
                assertEquals(3, wire.getUnsignedShortLE(2));
                assertEquals(request.getSerializedSize(), wire.getUnsignedIntLE(32));
                assertEquals(requestId, wire.getLongLE(36));

                assertFalse(decoder.writeInbound(wire.readRetainedSlice(36)));
                assertFalse(decoder.writeInbound(wire.readRetainedSlice(8)));
                assertTrue(decoder.writeInbound(wire.readRetainedSlice(request.getSerializedSize())));
                NsHeadFrame decoded = decoder.readInbound();
                assertEquals(requestId, decoded.requestId());
                assertEquals(request, EchoRequest.parseFrom(decoded.body()));
            } finally {
                wire.release();
            }
        } finally {
            encoder.finishAndReleaseAll();
            decoder.finishAndReleaseAll();
        }
    }
}
