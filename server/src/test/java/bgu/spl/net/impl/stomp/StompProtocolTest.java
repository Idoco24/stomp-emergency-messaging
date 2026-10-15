package bgu.spl.net.impl.stomp;

import bgu.spl.net.srv.ConnectionHandler;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class StompProtocolTest {
    static class Handler implements ConnectionHandler<String> {
        final List<String> messages = new ArrayList<>();
        public void send(String message) { messages.add(message); }
        public void close() { }
        String last() { return messages.get(messages.size() - 1); }
    }
    final ConnectionsImpl<String> connections = new ConnectionsImpl<>();
    Handler handler(int id) {
        Handler handler = new Handler(); connections.addConnection(id, handler); return handler;
    }
    StompProtocolImpl<String> protocol(int id) {
        StompProtocolImpl<String> protocol = new StompProtocolImpl<>(); protocol.start(id, connections); return protocol;
    }
    void login(StompProtocolImpl<String> protocol, String user) {
        protocol.process("CONNECT\naccept-version:1.2\nhost:stomp.cs.bgu.ac.il\nlogin:" + user + "\npasscode:demo\n\n");
    }

    @Test void routesSlashDestinationWithEachRecipientsSubscriptionId() {
        Handler a = handler(1), b = handler(2);
        StompProtocolImpl<String> pa = protocol(1), pb = protocol(2);
        login(pa, "alice"); login(pb, "bob");
        pa.process("SUBSCRIBE\ndestination:/alerts\nid:7\nreceipt:a\n\n");
        pb.process("SUBSCRIBE\ndestination:/alerts\nid:91\nreceipt:b\n\n");
        pa.process("SEND\ndestination:/alerts\n\n  body:with:colons\n\n");
        assertTrue(a.last().startsWith("MESSAGE\n"), a.last());
        assertTrue(a.last().contains("subscription:7\n"), a.last());
        assertTrue(b.last().contains("subscription:91\n"), b.last());
        assertTrue(b.last().contains("\n\n  body:with:colons\n\n"), b.last());
    }
    @Test void rejectsUnauthenticatedSubscription() {
        Handler h = handler(1); StompProtocolImpl<String> p = protocol(1);
        p.process("SUBSCRIBE\ndestination:alerts\nid:1\nreceipt:1\n\n");
        assertTrue(h.last().startsWith("ERROR\n")); assertTrue(p.shouldTerminate());
    }
    @Test void malformedHeaderIsAnErrorRatherThanAnException() {
        Handler h = handler(1); StompProtocolImpl<String> p = protocol(1);
        assertDoesNotThrow(() -> p.process("CONNECT\naccept-version\n\n"));
        assertTrue(h.last().startsWith("ERROR\n"));
    }
    @Test void credentialsContainingColonsAreNotTruncated() {
        Handler h = handler(1); StompProtocolImpl<String> p = protocol(1);
        p.process("CONNECT\naccept-version:1.2\nhost:stomp.cs.bgu.ac.il\nlogin:alice\npasscode:demo:secret\n\n");
        assertTrue(h.last().startsWith("CONNECTED\n"));
        connections.disconnect(1);
        assertTrue(connections.login("alice", "demo:secret", 2).startsWith("CONNECTED"));
    }
    @Test void errorDoesNotEchoPassword() {
        Handler h = handler(1); StompProtocolImpl<String> p = protocol(1);
        p.process("CONNECT\naccept-version:0.0\nhost:stomp.cs.bgu.ac.il\nlogin:alice\npasscode:very-private-demo\n\n");
        assertFalse(h.last().contains("very-private-demo"));
    }
    @Test void disconnectReleasesLoginAndSubscriptions() {
        handler(1); StompProtocolImpl<String> p = protocol(1); login(p, "alice");
        connections.subscribe("alerts", 1, 1); connections.disconnect(1);
        assertFalse(connections.checkSubscription("alerts", 1));
        assertTrue(connections.login("alice", "demo", 2).startsWith("CONNECTED"));
    }
    @Test void rejectsDuplicateSubscriptionId() {
        Handler h = handler(1); StompProtocolImpl<String> p = protocol(1); login(p, "alice");
        p.process("SUBSCRIBE\ndestination:alerts\nid:7\nreceipt:1\n\n");
        p.process("SUBSCRIBE\ndestination:news\nid:7\nreceipt:2\n\n");
        assertTrue(h.last().startsWith("ERROR\n"));
    }
    @Test void removesOnlyTheRequestedSubscriptionOnSameChannel() {
        Handler h = handler(1); connections.subscribe("alerts", 1, 1); connections.subscribe("alerts", 2, 1);
        assertTrue(connections.unsubscribe(1, 1));
        connections.send("alerts", "MESSAGE\ndestination:/alerts\n\nhello\0");
        assertEquals(1, h.messages.size()); assertTrue(h.last().contains("subscription:2\n"));
    }
    @Test void acceptsCrLfHeaders() {
        Handler h = handler(1); StompProtocolImpl<String> p = protocol(1);
        p.process("CONNECT\r\naccept-version:1.2\r\nhost:stomp.cs.bgu.ac.il\r\nlogin:alice\r\npasscode:demo\r\n\r\n");
        assertTrue(h.last().startsWith("CONNECTED\n"));
    }
    @Test void headerLikeBodyTextDoesNotChangeDestination() {
        Handler h = handler(1); StompProtocolImpl<String> p = protocol(1); login(p, "alice");
        p.process("SUBSCRIBE\ndestination:alerts\nid:7\nreceipt:1\n\n");
        p.process("SEND\ndestination:/alerts\n\ndestination:/other\n");
        assertTrue(h.last().startsWith("MESSAGE\n"), h.last());
    }
}
