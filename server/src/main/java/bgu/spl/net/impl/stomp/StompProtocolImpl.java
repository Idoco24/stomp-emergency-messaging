package bgu.spl.net.impl.stomp;

import bgu.spl.net.api.StompMessagingProtocol;
import bgu.spl.net.srv.Connections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Coursework STOMP subset: authentication, subscriptions, text messages and receipts. */
public class StompProtocolImpl<T> implements StompMessagingProtocol<T> {
    private boolean shouldTerminate;
    private boolean authenticated;
    private Connections<T> connections;
    private int connectionId;

    public void start(int connectionId, Connections<T> connections) {
        this.connectionId = connectionId;
        this.connections = connections;
    }

    public T process(T message) {
        if (shouldTerminate) return null;
        Frame frame = null;
        try {
            frame = parse((String) message);
            if (frame.command.equals("CONNECT") || frame.command.equals("STOMP")) {
                if (authenticated) throw new IllegalArgumentException("Already connected");
                connect(frame);
            } else {
                if (!authenticated) throw new IllegalArgumentException("CONNECT is required before other commands");
                switch (frame.command) {
                    case "SUBSCRIBE":
                        connections.subscribe(required(frame, "destination"), id(frame), connectionId);
                        receipt(frame);
                        break;
                    case "UNSUBSCRIBE":
                        if (!connections.unsubscribe(id(frame), connectionId))
                            throw new IllegalArgumentException("Unknown subscription id");
                        receipt(frame);
                        break;
                    case "SEND":
                        String destination = ConnectionsImpl.destination(required(frame, "destination"));
                        if (!connections.checkSubscription(destination, connectionId))
                            throw new IllegalArgumentException("Subscribe before sending to this destination");
                        connections.send(destination, cast("MESSAGE\ndestination:" + destination + "\n\n" + frame.body + "\0"));
                        receipt(frame);
                        break;
                    case "DISCONNECT":
                        receipt(frame);
                        terminate();
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported command");
                }
            }
        } catch (IllegalArgumentException error) {
            String receipt = frame == null ? null : frame.headers.get("receipt");
            // Never echo an incoming CONNECT frame or passcode in diagnostics.
            String reply = "ERROR\nmessage:Invalid frame\n"
                    + (receipt == null ? "" : "receipt-id:" + receipt + "\n")
                    + "\n" + error.getMessage() + "\0";
            connections.send(connectionId, cast(reply));
            terminate();
        }
        return null;
    }

    private void connect(Frame frame) {
        if (!"1.2".equals(required(frame, "accept-version")))
            throw new IllegalArgumentException("This coursework server supports version 1.2");
        if (!"stomp.cs.bgu.ac.il".equals(required(frame, "host")))
            throw new IllegalArgumentException("Unknown virtual host");
        String result = connections.login(required(frame, "login"), required(frame, "passcode"), connectionId);
        if (!result.startsWith("CONNECTED\n")) throw new IllegalArgumentException(result);
        authenticated = true;
        connections.send(connectionId, cast(result));
    }

    private void receipt(Frame frame) {
        String value = frame.headers.get("receipt");
        if (value != null) connections.send(connectionId, cast("RECEIPT\nreceipt-id:" + value + "\n\n\0"));
    }

    private void terminate() {
        shouldTerminate = true;
        authenticated = false;
        // The socket handler flushes queued replies before closing the transport.
        connections.disconnect(connectionId);
    }

    public boolean shouldTerminate() { return shouldTerminate; }

    @SuppressWarnings("unchecked")
    private T cast(String message) { return (T) message; }

    private static String required(Frame frame, String key) {
        String value = frame.headers.get(key);
        if (value == null || value.isEmpty()) throw new IllegalArgumentException("Missing header: " + key);
        return value;
    }

    private static int id(Frame frame) {
        int id = Integer.parseInt(required(frame, "id"));
        if (id < 0) throw new IllegalArgumentException("Subscription id must be non-negative");
        return id;
    }

    private static Frame parse(String message) {
        int boundary = message.indexOf("\r\n\r\n");
        int separatorLength = 4;
        if (boundary < 0) { boundary = message.indexOf("\n\n"); separatorLength = 2; }
        if (boundary < 0) throw new IllegalArgumentException("Missing header/body separator");
        String[] lines = message.substring(0, boundary).split("\r?\n");
        if (lines.length == 0 || lines[0].isEmpty()) throw new IllegalArgumentException("Missing command");
        Map<String, String> headers = new LinkedHashMap<>();
        for (int i = 1; i < lines.length; i++) {
            int colon = lines[i].indexOf(':');
            if (colon <= 0) throw new IllegalArgumentException("Malformed header");
            headers.putIfAbsent(lines[i].substring(0, colon), lines[i].substring(colon + 1));
        }
        return new Frame(lines[0], headers, message.substring(boundary + separatorLength));
    }

    private record Frame(String command, Map<String, String> headers, String body) { }
}
