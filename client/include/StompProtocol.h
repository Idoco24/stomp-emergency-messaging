#pragma once

#include "../include/ConnectionHandler.h"
#include <map>
#include <string>
#include "event.h"

class StompProtocol {
public:
    StompProtocol(ConnectionHandler& connectionHandler);
    void processFrame(const std::string& frame);
    std::string createConnectFrame(const std::string& host, const std::string& login, const std::string& passcode);
    std::string createSubscribeFrame(const std::string& destination, int id, int receipt);
    std::string createUnsubscribeFrame(const std::string channel_name, int receipt);
    std::string createSendFrame(const std::string& destination,const std::string& username, const std::string& body);
    std::string createDisconnectFrame(int receipt);

    std::string createSummary(const std::string& channel_name, const std::string& user, const std::vector<Event>& events);

    bool isConnected() const { return connected;} 
    
    void addSubscription(const std::string& channelName, int subscription_id) {
        subscriptions[channelName] = subscription_id;
    }
    
    int getSubscriptionId(const std::string& channel_name) {
        return subscriptions[channel_name];
    }
    
    void addCommand(std::string command, int receipts_id) {
        receipts[receipts_id] = command;
    }
    void RemoveSubscription(const std::string& channelName) {
        subscriptions.erase(channelName);
    }


private:
    ConnectionHandler& connectionHandler;
    bool connected;
    std::map<std::string, int> subscriptions; // channel_name -> subscription_id
    std::map<int, std::string> receipts; // receipt_id  -> command
};
