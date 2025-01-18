package bgu.spl.net.srv;

import bgu.spl.net.impl.stomp.StompMessage;
import bgu.spl.net.impl.stomp.Client;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class ConnectionsImpl<T> implements Connections<T> {
    
    private ConcurrentHashMap<Client, ConnectionHandler<T>> clients = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, List<Integer>> topics = new ConcurrentHashMap<>();
    
    private static ConnectionsImpl<StompMessage> instance;

    public static synchronized ConnectionsImpl<StompMessage> getInstance(){
        if (instance == null){
            instance = new ConnectionsImpl<>();
        }
        return instance;
    }

    @Override
    public boolean send(int connectionId, T msg) {
        Client client = getClient(connectionId);
        ConnectionHandler<T> handler = clients.get(client);
        if (handler != null) {
            handler.send(msg);
            return true;
        }
        return false;
    }

    private Client getClient(int connectionId) {
        for (Client client : clients.keySet()) {
            if (connectionId == client.getConnectionId()){
                return client;
            }
        }
    }

    @Override
    public void send(String channel, T msg) {
        List<Integer> subscribers = topics.get(channel);
        if (subscribers != null) {
            for (Integer id : subscribers) {
                send(id, msg);
            }
        }
    }

    @Override
    public void disconnect(int connectionId) {
        try {
            clients.get(connectionId).close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        clients.remove(connectionId);
        for (List<Integer> subscribers : topics.values()) {
            subscribers.remove(connectionId);
        }
    }

    public void subscribe(int connectionId, String channel) {
        if (!topics.containsKey(channel)) {
            topics.put(channel, new ArrayList<>());
            System.out.println("Channel '" + channel + "' created");
        }

        if (topics.get(channel).contains(connectionId)) {
            System.out.println("Error: the client " + connectionId + " is already subscribed to '" + channel + "' channel");
            return;
        } else {
            topics.get(channel).add(connectionId);
        }
        System.out.println("Client " + connectionId + " subscribed to '" + channel + "' channel");
    }

    public void unsubscribe(int connectionId, String channel) {
        List<Integer> subscribers = topics.get(channel);
        if (subscribers != null && subscribers.contains(connectionId)) {
            subscribers.remove(connectionId);
            System.out.println("Client " + connectionId + " unsubscribed from '" + channel + "' channel");
        } else {
            System.out.println("Error: the client " + connectionId + " is not subscribed to '" + channel + "' channel");
        }
    }

    public void addClient(int connectionId, ConnectionHandler<T> handler) {
        clients.put(connectionId, handler);
        System.out.println("Client " + connectionId + " connected");
    }

    public int checkClient()
}