package bgu.spl.net.impl.stomp;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class FrameCodecTest {
    @Test void preservesFragmentedUtf8Frame() {
        StompMessageEncodeDecoderImpl codec = new StompMessageEncodeDecoderImpl();
        String frame = "SEND\ndestination:/alerts\n\nשלום";
        for (byte b : frame.getBytes(StandardCharsets.UTF_8)) assertNull(codec.decodeNextByte(b));
        assertEquals(frame, codec.decodeNextByte((byte) 0));
    }
    @Test void heartbeatBeforeFrameIsIgnored() {
        StompMessageEncodeDecoderImpl codec = new StompMessageEncodeDecoderImpl();
        String result = null;
        for (byte b : "\n\r\nDISCONNECT\nreceipt:1\n\n\0".getBytes(StandardCharsets.UTF_8)) {
            String next = codec.decodeNextByte(b); if (next != null) result = next;
        }
        assertEquals("DISCONNECT\nreceipt:1\n\n", result);
    }
    @Test void encodingHasExactlyOneTrailingNull() {
        StompMessageEncodeDecoderImpl codec = new StompMessageEncodeDecoderImpl();
        assertArrayEquals("RECEIPT\n\n\0".getBytes(StandardCharsets.UTF_8), codec.encode("RECEIPT\n\n\0"));
    }
}
