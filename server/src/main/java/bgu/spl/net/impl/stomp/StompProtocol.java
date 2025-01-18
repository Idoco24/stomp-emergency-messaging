package bgu.spl.net.impl.stomp;

import bgu.spl.net.api.StompMessagingProtocol;
import bgu.spl.net.srv.ConnectionsImpl;
import bgu.spl.net.impl.stomp.StompMessage;
import bgu.spl.net.impl.stomp.KeyValuePair;

public class StompProtocol implements StompMessagingProtocol<StompMessage> {

    private boolean shouldTerminate = false;
    private int connectionId;
    private ConnectionsImpl<StompMessage> connections = ConnectionsImpl.getInstance();
    private int id = 0;

    @Override
    public void process(StompMessage message) {
        String header = message.getHeader();
        switch(header){
            case ("CONNECT"){
                connect(message) 
            }
        }
    }

    public void connect(StompMessage msg){
        String username;
        String password;

        for (KeyValuePair KeyValue : msg.getValues()){
            if (KeyValue.getKey().equals("login")){
                username = KeyValue.getValue();
            }
            if (KeyValue.getKey().equals("password")) {
                password = KeyValue.getValue();
            }
        }
        if (connections.getUsers().containsKey(username)){
            if (connections.getUsers().get(username).equals(password)){
                connections.addClient(id, connectionId);
                id++;
                connections.send(connectionId, new StompMessage("CONNECTED", new KeyValuePair("version", "1.2")));
            }
            else{
                connections.send(connectionId, new StompMessage("ERROR", new KeyValuePair("message", "Wrong password")));
                connections.disconnect(connectionId);
            }
        }
        else{
            connections.addUser(username, password);
            connections.addClient(id, connectionId);
            id++;
            connections.send(connectionId, new StompMessage("CONNECTED", new KeyValuePair("version", "1.2")));
        }
        
    }
}
