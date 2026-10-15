package bgu.spl.net.srv;

import bgu.spl.net.api.MessageEncoderDecoder;
import bgu.spl.net.api.MessagingProtocol;

import bgu.spl.net.impl.stomp.ConnectionsImpl;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class NonBlockingConnectionHandler<T> implements ConnectionHandler<T> {

    private static final int BUFFER_ALLOCATION_SIZE = 1 << 13; //8k
    private static final ConcurrentLinkedQueue<ByteBuffer> BUFFER_POOL = new ConcurrentLinkedQueue<>();

    private final MessagingProtocol<T> protocol;
    private final MessageEncoderDecoder<T> encdec;
    private final Queue<ByteBuffer> writeQueue = new ConcurrentLinkedQueue<>();
    private final SocketChannel chan;
    private final Reactor reactor;
    private boolean alreadyUp = false;
    private Connections<T> connections;
    private final int connectionId;

    public NonBlockingConnectionHandler(
            Connections<T> connections,
            MessageEncoderDecoder<T> reader,
            MessagingProtocol<T> protocol,
            SocketChannel chan,
            Reactor reactor, int connectionId) {
        this.chan = chan;
        this.encdec = reader;
        this.protocol = protocol;
        this.reactor = reactor;
        this.connections = connections;
        this.connectionId = connectionId;
    }

    public Runnable continueRead() {
        ByteBuffer buf = leaseBuffer();
        boolean success = false;
        try {
            if (chan.isOpen() && chan.isConnected()) {
                success = chan.read(buf) != -1;
            }
            else{
                close();
                return null;
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        if (success) {
            buf.flip();
            return () -> {
                try {
                    while (buf.hasRemaining() && !protocol.shouldTerminate()) {
                        T nextMessage = encdec.decodeNextByte(buf.get());
                        if (nextMessage != null) {
                            if(!alreadyUp){
                        
                                connections.addConnection(connectionId, this);
                                this.getProtocol().start(connectionId, connections);
                                alreadyUp = true;
                            }
                            T response = protocol.process(nextMessage);
                            if (response != null) {
                                writeQueue.add(ByteBuffer.wrap(encdec.encode(response)));
                                reactor.updateInterestedOps(chan, SelectionKey.OP_READ | SelectionKey.OP_WRITE);
                            }
                        }
                    }
                } finally {
                    // A final reply may already have drained before process() set
                    // termination. Wake the selector again so closure cannot be lost.
                    if (protocol.shouldTerminate() && !isClosed()) {
                        reactor.updateInterestedOps(chan, SelectionKey.OP_WRITE);
                    }
                    releaseBuffer(buf);
                }
            };
        } else {
            releaseBuffer(buf);
            close();
            return null;
        }

    }

    public void close() {
        connections.disconnect(connectionId);
        if (!isClosed()) {
            try {
                chan.close();
                // System.out.println("[DEBUG] Connection closed safely.");
            } catch (IOException e) {
                System.out.println("[ERROR] Closing connection failed: " + e.getMessage());
            }
        }
    }
    
    public boolean isClosed() {
        return !chan.isOpen();
    }

    public void continueWrite() {
        while (!writeQueue.isEmpty()) {
            try {
                ByteBuffer top = writeQueue.peek();
                if (top!= null){
                chan.write(top);
                    if (top.hasRemaining()) {
                        return;
                    } else {
                        writeQueue.remove();
                    }
                }
            } catch (IOException ex) {
                ex.printStackTrace();
                close();
            }
        }

        if (writeQueue.isEmpty()) {
            if (protocol.shouldTerminate()) close();
            else reactor.updateInterestedOps(chan, SelectionKey.OP_READ);
        }
    }

    private static ByteBuffer leaseBuffer() {
        ByteBuffer buff = BUFFER_POOL.poll();
        if (buff == null) {
            return ByteBuffer.allocateDirect(BUFFER_ALLOCATION_SIZE);
        }

        buff.clear();
        return buff;
    }

    private static void releaseBuffer(ByteBuffer buff) {
        BUFFER_POOL.add(buff);
    }

    @Override
    public void send(T msg) {
        
        // System.out.println("Message to encode " + msg);
        
        writeQueue.add(ByteBuffer.wrap(encdec.encode(msg)));
        reactor.updateInterestedOps(chan, SelectionKey.OP_READ | SelectionKey.OP_WRITE);

    }

    public MessagingProtocol<T> getProtocol() {
        return protocol;
    }
}
