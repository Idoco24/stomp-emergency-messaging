package bgu.spl.net.impl.stomp;

public class Client {
    private String username;
    private String passcode;
    private int connectionId;

    public Client(String username, String passcode, int connectionId){
        this.username = username;
        this.passcode = passcode;
        this.connectionId = connectionId;
    }

    public String getUsername(){
        return username;
    }

    public String getPasscode(){
        return passcode;
    }

    public int getConnectionId(){
        return connectionId;
    }
}

