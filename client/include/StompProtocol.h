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
    std::string createUnsubscribeFrame(int id, int receipt);
    std::string createSendFrame(const std::string& destination, const std::string& body);
    std::string createDisconnectFrame(int receipt);

    std::string createSummary(const std::string& channel_name, const std::string& user, const std::vector<Event>& events);

    bool isConnected() const { return connected;} 
    
    void addSubscription(const std::string& game_name, int subscription_id) {
        subscriptions[game_name] = subscription_id;
    }
    
    int getSubscriptionId(const std::string& game_name) {
        return subscriptions[game_name];
    }
    
    void addEvent(const std::string& game_name, const Event& event) {
        gameEvents[game_name].push_back(event);
    }



private:
    ConnectionHandler& connectionHandler;
    bool connected;
    std::map<std::string, int> subscriptions; // game_name -> subscription_id
    std::map<std::string, std::vector<Event>> gameEvents; // game_name -> events
};
