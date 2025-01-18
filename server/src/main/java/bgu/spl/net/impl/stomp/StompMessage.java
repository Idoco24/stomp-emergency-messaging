package bgu.spl.net.impl.stomp;

import bgu.spl.net.api.MessageEncoderDecoder;
import java.util.List;
import bgu.spl.net.impl.stomp.KeyValuePair;

public class StompMessage {

    private final String header;
    private final List<KeyValuePair> values;
    private final String body;

    public StompMessage(String header, List<KeyValuePair> values, String body) {
        this.header = header;
        this.values = values;
        this.body = body;
    }

    public String getHeader() {
        return header;
    }

    public List<KeyValuePair> getValues() {
        return values;
    }

    public String getBody() {
        return body;
    }

}