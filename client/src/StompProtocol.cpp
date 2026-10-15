#include "../include/StompProtocol.h"
#include <fstream>
#include <string>
#include <vector>
#include "../include/json.hpp"
#include <sstream>
#include <iostream>
#include <filesystem>
using json = nlohmann::json;

StompProtocol::StompProtocol(ConnectionHandler& connectionHandler) 
    : connectionHandler(connectionHandler), connected(false), username(), subscriptions(), receipts(), recordedEvents() {}

std::string StompProtocol::createConnectFrame(const std::string& host, const std::string& login, const std::string& passcode) {
    username = login;
    std::ostringstream frame;
    frame << "CONNECT\n"
          << "accept-version:1.2\n"
          << "host:" << host << "\n"
          << "login:" << login << "\n"
          << "passcode:" << passcode << "\n\n";
        //   << '\0';
    // connectionHandler.sendFrameAscii("", '\0');
    return frame.str();
}

std::string StompProtocol::createSubscribeFrame(const std::string& destination, int id, int receipt) {

    if (subscriptions.find(destination) != subscriptions.end()) {
        id = subscriptions[destination];
    }
    
    std::ostringstream frame;
    frame << "SUBSCRIBE"<< "\n"
        //   << "destination:/" << destination << "\n"
          << "destination:" << destination << "\n"
          << "id:" << id << "\n"
          << "receipt:" << receipt << "\n\n";
        //   << '\0';
    
    subscriptions[destination] = id;
    receipts[receipt] = "join " + destination;
    
    // connectionHandler.sendFrameAscii("", '\0');
    // std::cout << "send join with id: " << id << std::endl;
    // std::cout << "send join with reccipt: " << receipt << std::endl;
    return frame.str();
}

bool StompProtocol::isSubscribed(const std::string& channel_name) {
    if (subscriptions.find(channel_name) == subscriptions.end()) {
        return false;
    }
    return true;

}

std::string StompProtocol::createUnsubscribeFrame(const std::string channel_name, int receipt) {
    if (subscriptions.find(channel_name) == subscriptions.end()) {
        std::cout << "you are not subscribed to channel " << channel_name << std::endl;
        return "";
    }

    int id = subscriptions[channel_name];
    
    std::ostringstream frame;
    frame << "UNSUBSCRIBE\n"
          << "id:" << id << "\n"
          << "receipt:" << receipt << "\n\n";
        //   << '\0';

    // connectionHandler.sendFrameAscii("", '\0');


    receipts[receipt] = "exit " + channel_name;

    // std::cout << "send exit with id: " << id << std::endl;
    // std::cout << "send exit with reccipt: " << receipt << std::endl;
    return frame.str();
}

std::string StompProtocol::createSendFrame(const Event& event) {
    std::string destination = event.get_channel_name();
    if (subscriptions.find(destination) == subscriptions.end()) {
        std::cerr << "you are not subscribed to channel " << destination << std::endl;
        return "";
    }
    std::string event_details = event.toString();
    std::ostringstream frame;
    frame << "SEND" << "\n"
          << event_details << "\n";
        //   << '\0';
    
    // connectionHandler.sendFrameAscii("", '\0');

    // std::cout << "Sending frame" << frame.str() << std::endl;
    return frame.str();
}

std::string StompProtocol::createDisconnectFrame(int receipt) {
    std::ostringstream frame;
    frame << "DISCONNECT\n"
          << "receipt:" << receipt << "\n\n";
        //   << '\0';
    
    // connectionHandler.sendFrameAscii("", '\0');

    receipts[receipt] = "logout";
    return frame.str();
}

void StompProtocol::createSummary(const std::string& channel_name, const std::string& user, const std::vector<Event>& events, std::string& outpath) {
    if (subscriptions.find(channel_name) == subscriptions.end()) {
        std::cout << "You are not subscribed to channel " << channel_name << std::endl;
        return;
    }
    
    std::ofstream outFile(outpath);
    if (!outFile.is_open()) {
        std::cerr << "Unable to open file for writing: " << outpath << std::endl;
        return;
    }

    outFile << "Channel " << channel_name << "\n";
    outFile << "User " << user << "\n";
    outFile << "Stats:\n";

    int totalEvents = events.size();
    int activeCount = 0;
    int forcesArrivalCount = 0;

    for (const auto& event : events) {
        std::map<std::string, std::string> generalInfo = event.get_general_information();
        
        if (generalInfo["active"].find("true") != std::string::npos) {
            activeCount++;
        }
        if (generalInfo["forces_arrival_at_scene"].find("true") != std::string::npos) {
            forcesArrivalCount++;
        }
    }

    outFile << "Total: " << totalEvents << "\n";
    if (activeCount != 0 ) {
        outFile << "active: " << activeCount << "\n";
    }else{
        outFile << "\n";
    }
    if (forcesArrivalCount != 0) {
        outFile << "forces arrival at scene: " << forcesArrivalCount << "\n";
    }else {
        outFile << "\n";
    }

    outFile << "Event Reports:\n\n";

    for (const auto& event : events) {
        outFile << epochToDate(event.get_date_time()) << " - " << event.get_name() << " - " << event.get_city() << ":\n";
        outFile << event.get_description() << "\n";
    }

    outFile.close();
    // std::cout << "Summary written to " << outpath << std::endl;
}

void StompProtocol::processFrame(const std::string& frame) {
    if (frame.rfind("CONNECTED\n", 0) == 0) {
        connected = true;
        std::cout << "Login successful" << std::endl;

    } else if (frame.rfind("RECEIPT\n", 0) == 0) {
        std::istringstream iss(frame);
        std::string header, receipt_line;
        iss >> header >> receipt_line;
        size_t location = receipt_line.find(':');
        std::string receipt_id = receipt_line.substr(location+1);
        int receipt = std::stoi(receipt_id);
        std::string command = receipts[receipt];

        if (command == "logout") {
            connected = false;
            std::cout << "Logged out" << std::endl;
            connectionHandler.close();
        }

        else if (command.find("exit") != std::string::npos) {
            std::string channel_name = command.substr(5);
            subscriptions.erase(channel_name);
            std::cout << "Exited from " << channel_name << std::endl;
        }
        else if (command.find("join") != std::string::npos) {
            std::string channel_name = command.substr(5);
            std::cout << "Joined channel " << channel_name << std::endl;
        }
    }else if (frame.rfind("MESSAGE\n", 0) == 0) {
        Event event(frame);

        
        if (recordedEvents.find(event.get_channel_name()) == recordedEvents.end()) {
            recordedEvents[event.get_channel_name()] = std::vector<Event>();
        }
        recordedEvents[event.get_channel_name()].push_back(event);
    } 

    if (frame.rfind("ERROR\n", 0) == 0) {
        std::cout << "ERROR FROM THE SERVER:\n\n" << frame << std::endl;
        connectionHandler.close();
        connected = false;
    }
}
