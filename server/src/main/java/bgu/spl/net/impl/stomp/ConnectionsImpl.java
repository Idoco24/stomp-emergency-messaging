package bgu.spl.net.impl.stomp;

import bgu.spl.net.srv.ConnectionHandler;
import bgu.spl.net.srv.Connections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnectionsImpl<T> implements Connections<T> {
    private final Map<Integer, ConnectionHandler<T>> clients = new ConcurrentHashMap<>();
    private final Map<String, String> users = new ConcurrentHashMap<>();
    private final Map<Integer, String> activeUsers = new ConcurrentHashMap<>();
    private final Map<Integer, Map<Integer, String>> subscriptions = new ConcurrentHashMap<>();
    private final AtomicInteger nextMessageId = new AtomicInteger();

    static String destination(String channel) { return channel.startsWith("/") ? channel : "/" + channel; }

    public boolean send(int connectionId, T message) {
        ConnectionHandler<T> handler = clients.get(connectionId);
        if (handler == null) return false;
        handler.send(message);
        return true;
    }

    @SuppressWarnings("unchecked")
    public synchronized void send(String channel, T message) {
        String target = destination(channel);
        String frame = (String) message;
        for (Map.Entry<Integer, Map<Integer, String>> client : subscriptions.entrySet()) {
            for (Map.Entry<Integer, String> subscription : client.getValue().entrySet()) {
                if (target.equals(subscription.getValue())) {
                    String reply = "MESSAGE\nmessage-id:" + nextMessageId.getAndIncrement()
                            + "\nsubscription:" + subscription.getKey() + "\n" + frame.substring("MESSAGE\n".length());
                    send(client.getKey(), (T) reply);
                }
            }
        }
    }

    public synchronized String login(String username, String password, int connectionId) {
        if (activeUsers.containsKey(connectionId)) return "Already connected";
        if (users.containsKey(username) && !users.get(username).equals(password)) return "Wrong password";
        if (activeUsers.containsValue(username)) return "User already logged in";
        users.putIfAbsent(username, password);
        activeUsers.put(connectionId, username);
        return "CONNECTED\nversion:1.2\n\n\0";
    }

    public void addConnection(int connectionId, ConnectionHandler<T> handler) { clients.put(connectionId, handler); }

    public synchronized void subscribe(String channel, int subscriptionId, int connectionId) {
        Map<Integer, String> client = subscriptions.computeIfAbsent(connectionId, key -> new ConcurrentHashMap<>());
        if (client.putIfAbsent(subscriptionId, destination(channel)) != null)
            throw new IllegalArgumentException("Subscription id is already in use");
    }

    public synchronized boolean unsubscribe(int subscriptionId, int connectionId) {
        Map<Integer, String> client = subscriptions.get(connectionId);
        return client != null && client.remove(subscriptionId) != null;
    }

    public synchronized void disconnect(int connectionId) {
        clients.remove(connectionId);
        subscriptions.remove(connectionId);
        activeUsers.remove(connectionId);
    }

    public synchronized void removeActiveUser(int connectionId) { activeUsers.remove(connectionId); }

    public synchronized boolean checkSubscription(String channel, int connectionId) {
        Map<Integer, String> client = subscriptions.get(connectionId);
        return client != null && client.containsValue(destination(channel));
    }
}
