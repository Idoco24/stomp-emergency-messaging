package bgu.spl.net.srv;

import java.io.IOException;

public interface Connections<T> {

    boolean send(int connectionId, T msg);

    void send(String channel, T msg);

    void disconnect(int connectionId);

    void addConnection(int connectionId, ConnectionHandler<T> handler);

    String login(String username, String password, int connectionId);

    void subscribe(String channel, int subscriptionId ,int connectionId);

    boolean unsubscribe(int subscriptionId, int connectionId);
    
    public void removeActiveUser(int connectionId);

    public boolean checkSubscription(String destination, int connectionId);
}
