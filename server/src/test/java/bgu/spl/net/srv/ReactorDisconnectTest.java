package bgu.spl.net.srv;

import bgu.spl.net.impl.stomp.ConnectionsImpl;
import bgu.spl.net.impl.stomp.StompMessageEncodeDecoderImpl;
import bgu.spl.net.impl.stomp.StompProtocolImpl;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.junit.jupiter.api.Assertions.*;

class ReactorDisconnectTest {
    /** Force the selector to flush the receipt before process() marks termination. */
    static class ImmediateWriter extends Reactor<String> {
        NonBlockingConnectionHandler<String> handler;
        ImmediateWriter() { super(1, 0, StompProtocolImpl::new, StompMessageEncodeDecoderImpl::new); }
        @Override void updateInterestedOps(SocketChannel channel, int ops) {
            if ((ops & SelectionKey.OP_WRITE) != 0) handler.continueWrite();
        }
    }

    @Test @Timeout(5)
    void socketClosesEvenIfReceiptWasFlushedBeforeTerminationWasMarked() throws Exception {
        try (ServerSocketChannel listener = ServerSocketChannel.open()) {
            listener.bind(new InetSocketAddress("127.0.0.1", 0));
            try (Socket peer = new Socket("127.0.0.1", listener.socket().getLocalPort());
                    SocketChannel channel = listener.accept()) {
                ImmediateWriter reactor = new ImmediateWriter();
                reactor.handler = new NonBlockingConnectionHandler<>(new ConnectionsImpl<>(),
                        new StompMessageEncodeDecoderImpl(), new StompProtocolImpl<>(), channel, reactor, 1);
                peer.getOutputStream().write(("CONNECT\naccept-version:1.2\nhost:stomp.cs.bgu.ac.il\n"
                        + "login:alice\npasscode:demo\n\n\0").getBytes(StandardCharsets.UTF_8));
                reactor.handler.continueRead().run();
                while (peer.getInputStream().read() != 0) { }
                peer.getOutputStream().write("DISCONNECT\nreceipt:bye\n\n\0".getBytes(StandardCharsets.UTF_8));
                reactor.handler.continueRead().run();
                assertFalse(channel.isOpen(), "Receipt drain raced with protocol termination and left the socket open");
            }
        }
    }
}
