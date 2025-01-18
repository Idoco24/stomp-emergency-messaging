package bgu.spl.net.impl.stomp;

import bgu.spl.net.api.MessageEncoderDecoder;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;


public class StompMessageEncoderDecoder implements MessageEncoderDecoder<StompMessage>{

    private StringBuilder message = new StringBuilder();
    private boolean parsed = false;
    private String Header;
    private List<KeyValuePair> values = new ArrayList<>();
    private String body;

    @Override
    public StompMessage decodeNextByte(byte nextByte) {
        if (nextByte == '\u0000') {
            parseMassage();
            StompMessage result = new StompMessage(Header, values, body);
            reset();
            return result;
        }
        message.append((char)nextByte);
        return null;
    }

    @Override
    public byte[] encode(StompMessage message) {
        StringBuilder output = new StringBuilder();
        output.append(message.getHeader() + "\n");
        for (KeyValuePair pair : message.getValues()) {
            output.append(pair.getKey() + ":" + pair.getValue() + "\n");
        }
        output.append("\n").append(message.getBody()).append("\u0000");
        return output.toString().getBytes(StandardCharsets.UTF_8);
    }

    private void parseMassage() {
        String[] lines = message.toString().split("\n\n, 2");
        String[] headerAndValues = lines[0].split("\n");
         Header = headerAndValues[0];

        for (int i = 1; i < headerAndValues.length; i++) {
           String[] pair = headerAndValues[i].split(":",2);
            values.add(new KeyValuePair(pair[0], pair[1]));
        }
        if (lines.length > 1) {
            body = lines[1];
        }
    }

    private void reset() {
        message.setLength(0);
        parsed = false;
        Header = null;
        values.clear();
        body = null;
    }
}