package bgu.spl.net.impl.stomp;

import bgu.spl.net.srv.ConnectionHandler;
import bgu.spl.net.srv.Connections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ConnectionsImpl<T> implements Connections<T> {
    
    ConcurrentHashMap<Integer, ConnectionHandler<T>> clients = new ConcurrentHashMap<>(); // connectionId, handler
    ConcurrentHashMap<String, Set<Integer>> channels = new ConcurrentHashMap<>(); // channel, connectionId
    ConcurrentHashMap<String, String> users = new ConcurrentHashMap<>(); // username, password
    ConcurrentHashMap<Integer, String> ActiveUsers = new ConcurrentHashMap<>(); // username, connectionId
    private ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, String>> subscriptions = new ConcurrentHashMap<>(); // connectionId, subscriptionId, channel
    private int messageId = 0;
    
    @Override
    public boolean send(int connectionId, T msg) {
        ConnectionHandler<T> handler = clients.get(connectionId);
        // System.out.println("attempting to send message to "+ handler);
        
        if (handler != null){
            
            handler.send(msg);
            return true;
        }
        return false;
    }
    @Override
    public void send(String channel, T msg) {
        Set<Integer> subscribers = channels.get(channel);
        if (subscribers != null) {
            for (Integer connectionId : subscribers){
                String messageWithId = addMessageIdToMessage(msg, this.messageId++);
                send(connectionId, (T) messageWithId);
                System.out.println(messageWithId);
            }
        }
    }

    private String addMessageIdToMessage(T msg, int messageId) {
        String message = (String) msg;
        StringBuilder sb = new StringBuilder();
        sb.append("MESSAGE\n");
        sb.append("message-id:").append(messageId).append("\n");
        sb.append(message.substring(8));    // Skip the initial "MESSAGE\n" part of the original message
        
        // System.out.println("message after edit\n"+sb.toString());
        
        return sb.toString();
    }

    
    @Override
    public String login(String username, String password, int connectionId){
        // if the user is already logged in
        if (users.containsKey(username)){
            if(!users.get(username).equals(password)){
                return "User " + username + "'s password is different than what you inserted";
            }
            else if(ActiveUsers.containsValue(username)){
                return username + " is logged in somewhere else";
            }
        }
        ActiveUsers.put(connectionId, username);
        users.putIfAbsent(username, password);
        return "CONNECTED\nversion:1.2\n";
    }
    @Override   
    public void addConnection(int connectionId, ConnectionHandler<T> handler) {
        clients.put(connectionId, handler);
        // System.out.println("Connection added with id: " + connectionId + " and handler: " + handler);
    }
    
    @Override
    public void subscribe(String channel,int subscriptionId ,int connectionId) {
        channels.putIfAbsent(channel, ConcurrentHashMap.newKeySet());
        channels.get(channel).add(connectionId);

        subscriptions.putIfAbsent(connectionId, new ConcurrentHashMap<>());
        subscriptions.get(connectionId).put(subscriptionId, channel);
        // System.out.println("User " + connectionId + " subscribed to channel " + channel);
    }

    @Override
    public boolean unsubscribe(int subscriptionId, int connectionId) {
        ConcurrentHashMap<Integer, String> userSubscriptions = subscriptions.get(connectionId);
        
        if (userSubscriptions == null || !userSubscriptions.containsKey(subscriptionId)) {
            return false;
        }

        if (userSubscriptions != null) {
            String channel = userSubscriptions.remove(subscriptionId);
            if (channel != null) {
                Set<Integer> subscribers = channels.get(channel);
                if (subscribers != null) {
                    subscribers.remove(connectionId);

                    if (subscribers.isEmpty()) {
                        channels.remove(channel);
                        
                    }
                }
            }
        }
        return true;
    }

    @Override
    public void disconnect(int connectionId) {
        clients.remove(connectionId);
        ConcurrentHashMap<Integer, String> userSubscriptions = subscriptions.remove(connectionId);
        if (userSubscriptions != null) {
            for (String channel : userSubscriptions.values()) {
                Set<Integer> subscribers = channels.get(channel);
                if (subscribers != null) {
                    subscribers.remove(connectionId);

                    if (subscribers.isEmpty()) {
                        channels.remove(channel);
                    }
                }
            }
        }
    }

    @Override
    public void removeActiveUser(int connectionId){
        if (ActiveUsers.get(connectionId) != null){
            ActiveUsers.remove(connectionId);
        }
    }

    @Override 
    public boolean checkSubscription(String destination, int connectionId){
        ConcurrentHashMap<Integer, String> userSubscriptions = subscriptions.get(connectionId);
        if (userSubscriptions != null) {
            return userSubscriptions.containsValue(destination);
        }
        return false;
    }
}