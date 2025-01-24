#pragma once

#include "../include/ConnectionHandler.h"
#include <map>
#include <string>
#include "event.h"
#include <c++/10/iomanip>

class StompProtocol {
public:
    StompProtocol(ConnectionHandler& connectionHandler);
    void processFrame(const std::string& frame);
    std::string createConnectFrame(const std::string& host, const std::string& login, const std::string& passcode);
    std::string createSubscribeFrame(const std::string& destination, int id, int receipt);
    std::string createUnsubscribeFrame(const std::string channel_name, int receipt);
    std::string createSendFrame(const Event& event);
    std::string createDisconnectFrame(int receipt);

    void createSummary(const std::string& channel_name, const std::string& user, const std::vector<Event>& events, std::string& outpath);

    std::string epochToDate(time_t epoch) {
        std::tm *tm = std::localtime(&epoch);
        std::ostringstream oss;
        oss << std::put_time(tm, "%d/%m/%y %H:%M:%S");
        return oss.str();
}

    bool isConnected() const { return connected;} 
    
    void addSubscription(const std::string& channelName, int subscription_id) {
        subscriptions[channelName] = subscription_id;
    }
    
    bool isSubscribed(const std::string& channel_name);

    int getSubscriptionId(const std::string& channel_name) {
        return subscriptions[channel_name];
    }
    
    void addCommand(std::string command, int receipts_id) {
        receipts[receipts_id] = command;
    }
    void RemoveSubscription(const std::string& channelName) {
        subscriptions.erase(channelName);
    }
    void addEvent(const Event& event) {
        recordedEvents[event.get_channel_name()].push_back(event);
    }

    std::vector<Event> getEvents(const std::string& channel_name, std::string user) {
        std::vector<Event> eventus;
        for (const auto& event : recordedEvents[channel_name]) {
            if (event.getEventOwnerUser().find(user) != std::string::npos) {
                // std::cout << event.toString() << std::endl;
                eventus.push_back(event);
            }
        }
        return eventus;
    }


private:
    ConnectionHandler& connectionHandler;
    bool connected;
    std::map<std::string, int> subscriptions; // channel_name -> subscription_id
    std::map<int, std::string> receipts; // receipt_id  -> command
    std::map<std::string, std::vector<Event>> recordedEvents; // channel_name -> events
};
