package bgu.spl.net.impl.stomp;

import bgu.spl.net.api.StompMessagingProtocol;
import bgu.spl.net.srv.Connections;

public class StompProtocolImpl<T> implements StompMessagingProtocol<T> {

    private boolean shouldTerminate = false;
    private Connections<T> connections;
    private int connectionId;

    @Override
    public void start(int connectionId, Connections<T> connections) {
        this.connections = connections;
        this.connectionId = connectionId;
    }

    @Override
    public T process(T message) {
        // System.out.println(message); // Debugging message received
        String myMessage = (String) message;
        String[] lines = myMessage.split("\n");
        String command = lines[0];

        switch (command) {
            case "CONNECT":
                handleConnect(lines);
                break;
            case "SUBSCRIBE":
                handleSubscribe(lines);
                break;
            case "UNSUBSCRIBE":
                handleUnsubscribe(lines);
                break;
            case "DISCONNECT":
                handleDisconnect(lines);
                break;
            case "SEND":
                handleSend(lines);
                break;
            default:
                sendError("malformed frame received", "Unkown Frame Title" + command, lines);
        }

        return null;
    }

    @Override
    public boolean shouldTerminate() {
        return shouldTerminate;
    }

    private void handleConnect(String[] lines) {
        String frameToSend;
        String quicky= "malformed frame received";
        String version = null;
        String host = null;
        String user = null;
        String pass = null;

        for (String line : lines){
            if (line.contains("accept-version")){
                version = getHeaderValue(line);
            }
            else if (line.contains("host")){
                host = getHeaderValue(line);
            }
            else if (line.contains("login")){
                user = getHeaderValue(line);
            }
            else if (line.contains("passcode")){
                pass = getHeaderValue(line);
            }
        }
        
        if (!"1.2".equals(version)){
            if (version == null){
                frameToSend = "Did not contain a version header ,which is REQUIRED for message propagation.";
                quicky = "missing header";
            }
            else {
                frameToSend = "Version must be 1.2";
                quicky = "Unkown version";
            }
        }else if (!"stomp.cs.bgu.ac.il".equals(host)){
            
            if (host == null){
                frameToSend = "Did not contain a host header ,which is REQUIRED for message propagation.";
                quicky = "missing header";
            
            }else {
                frameToSend = "Host must be: stomp.cs.bgu.ac.il";
                quicky = "Unkown host";
            }

        }else if (user == null){
            frameToSend = "Did not contain a user header ,which is REQUIRED for message propagation.";
            quicky = "missing header";
        }   
        else if (pass == null){
            frameToSend = "Did not contain a pass header ,which is REQUIRED for message propagation.";
            quicky = "missing header";
        } else {
            frameToSend = connections.login(user, pass, connectionId);
        }
        
        if (frameToSend.contains("CONNECTED")){
            connections.send(connectionId, (T) frameToSend);
        }

        else {

            if (frameToSend.contains("password")){
                quicky = "Password does not math UserName";
            }
            else if (frameToSend.contains("somewhere else")){
                quicky = "User already logged in";
            }
            sendError(quicky, frameToSend, lines);
        }
    }

    private void handleSubscribe(String[] lines) {
        String destination = null;
        int subId = -1;
        String receipt = null;

        for (String line : lines){
            if (line.contains("destination")){
                destination = getHeaderValue(line);
            }
            else if (line.contains("id")){
                try{
                    subId = Integer.parseInt(getHeaderValue(line));
                }
                catch (Exception e){
                    sendError("Invalid id", "recipt-id should be integer only, got " + getHeaderValue(line), lines);
                    return;
                }
            }
            else if (line.contains("receipt")){
                receipt = getHeaderValue(line);
            }
        }

        if (destination != null && subId != -1 && receipt != null){
            connections.subscribe(destination, subId, connectionId);
            sendReceipt(receipt);
        }
        else if (destination == null){
            sendError("missing header", "Did not contain a destination header ,which is REQUIRED for message propagation.", lines);
        }
        else if (subId == -1){
            sendError("missing header", "Did not contain a id header ,which is REQUIRED for message propagation.", lines);
        }
        else if (receipt == null){
            sendError("missing header", "Did not contain a receipt header ,which is REQUIRED for message propagation.", lines);
        }
    }

    private void handleUnsubscribe(String[] lines) {
        int subId = -1;
        String receipt = null;
        
        for (String line : lines) {
            if (line.contains("id")) {
                try {
                    subId = Integer.parseInt(getHeaderValue(line));
                } catch (Exception e) {
                    sendError("Invalid id", "recipt-id should be integer only, got " + getHeaderValue(line), lines);
                    return;
                }
            } else if (line.contains("receipt")) {
                receipt = getHeaderValue(line);
            }
        }

        if (subId != -1 && receipt != null){
            boolean is_legit = connections.unsubscribe(subId, connectionId);
            
            if (!is_legit){
                sendError("you are not subscribed to this channelId", "You tried to unsubscribe from a channel you are not subscribed to", lines);
                return;
            }
            
            sendReceipt(receipt);
        }
        else if (subId == -1){
            sendError("missing header", "Did not contain a id header ,which is REQUIRED for message propagation.", lines);
        }
        else if (receipt == null){
            sendError("missing header", "Did not contain a receipt header ,which is REQUIRED for message propagation.", lines);
        }
    }


    private void handleSend(String[] lines) {
        String destination = null;
        String receiptId = null;
    
        for (String line : lines) {
            if (line.startsWith("destination:")) {
                destination = getHeaderValue(line).substring(1);
            } else if (line.startsWith("receipt:")) {
                receiptId = getHeaderValue(line);
            }
        }
    
        if (destination == null) {
            String quicky = "missing header";
            String description = "Missing required destination header in SEND command";
            sendError(quicky, description, lines);
            return;
        }

        if (!connections.checkSubscription(destination, connectionId)) {
            String quicky = "missing subscription for client";
            String description = "Client not subscribed to destination: " + destination;
            sendError(quicky, description, lines);
            return;
        }
    
        String body = extractBody(lines);
    
        String messageFrame = "MESSAGE\n" +
                              "subscription:" + connectionId + "\n" +
                              "destination:/" + destination + "\n\n" +
                              body + "\n\0";
    
        connections.send(destination, (T) messageFrame);
    
        if (receiptId != null) {
            sendReceipt(receiptId);
        }
    }
    
    private String extractBody(String[] lines) {
        StringBuilder bodyBuilder = new StringBuilder();
        boolean bodyStarted = false;
    
        for (String line : lines) {
            if (bodyStarted) {
                bodyBuilder.append(line).append("\n");
            } else if (line.isEmpty()) {
                bodyStarted = true;
            }
        }
    
        return bodyBuilder.toString().trim();
    }

    private void handleDisconnect(String[] lines) {
        String receipt = null;
        for (String line : lines){
            if (line.contains("receipt")){
                receipt = getHeaderValue(line);
            }
        }
        
        if (receipt == null){
            String quicky = "missing receipt header";
            String description = "Did not contain a receipt header ,which is REQUIRED for message propagation.";
            
            sendError(quicky, description, lines);
            return;
        }

        connections.removeActiveUser(connectionId);
        sendReceipt(receipt);
        connections.disconnect(connectionId);
        shouldTerminate = true;

    }
   
    @SuppressWarnings("unchecked")
    private void sendError(String quicky, String description ,String[] lines) {
        StringBuilder errorFrame = new StringBuilder();
        errorFrame.append("ERROR\n");
    
        // Add a message header with a short description of the error
        errorFrame.append("message: ").append(quicky).append("\n");
    
        // Optionally add a receipt-id header if provided in the lines
        for (String line : lines) {
            if (line.startsWith("receipt:")) {
                errorFrame.append("receipt-id:").append(getHeaderValue(line)).append("\n");
                break;
            }
        }
    
        // Add the body of the error frame
        errorFrame.append("\n");
        errorFrame.append("The message:\n-----\n");
        for (String line : lines) {
            errorFrame.append(line).append("\n");
        }
        errorFrame.append("-----\n");
        errorFrame.append(description).append("\n");
        errorFrame.append("\u0000");
        
        String errorMessage = errorFrame.toString();

        connections.send(connectionId, (T) errorMessage);

        connections.removeActiveUser(connectionId);
        connections.disconnect(connectionId);
        shouldTerminate = true;


    }


    @SuppressWarnings("unchecked")
    private void sendReceipt(String receipt) {
        String frameToSend =    "RECEIPT\n"+
                                "receipt-id:" + receipt + "\n\n\0";
        
        connections.send(connectionId, (T) frameToSend);
    }

    private String getHeaderValue(String header) {
        return header.split(":")[1].trim();
    }
}