#include "../include/StompProtocol.h"
#include <sstream>
#include <iostream>

StompProtocol::StompProtocol(ConnectionHandler& connectionHandler) 
    : connectionHandler(connectionHandler), connected(false), subscriptions(), receipts() {}

std::string StompProtocol::createConnectFrame(const std::string& host, const std::string& login, const std::string& passcode) {
    std::ostringstream frame;
    frame << "CONNECT\n"
          << "accept-version:1.2\n"
          << "host:" << host << "\n"
          << "login:" << login << "\n"
          << "passcode:" << passcode << "\n\n"
          << '\0';
    return frame.str();
}

std::string StompProtocol::createSubscribeFrame(const std::string& destination, int id, int receipt) {
    std::ostringstream frame;
    frame << "SUBSCRIBE"<< "\n"
          << "destination:/" << destination << "\n"
          << "id:" << id << "\n"
          << "receipt:" << receipt << "\n\n"
          << '\0';

    subscriptions[destination] = id;
    receipts[receipt] = "join " + destination;

    std::cout << "send join with id: " << id << std::endl;
    std::cout << "send join with reccipt: " << receipt << std::endl;
    return frame.str();
}

std::string StompProtocol::createUnsubscribeFrame(const std::string channel_name, int receipt) {
    if (subscriptions.find(channel_name) == subscriptions.end()) {
        std::cerr << "Error: Channel " << channel_name << " not found" << std::endl;
        return "";
    }

    int id = subscriptions[channel_name];
    
    std::ostringstream frame;
    frame << "UNSUBSCRIBE\n"
          << "id:" << id << "\n"
          << "receipt:" << receipt << "\n\n"
          << '\0';

    receipts[receipt] = "exit " + channel_name;

    std::cout << "send exit with id: " << id << std::endl;
    std::cout << "send exit with reccipt: " << receipt << std::endl;
    return frame.str();
}

std::string StompProtocol::createSendFrame(const std::string& destination, const std::string& username, const std::string& body) {
    if (subscriptions.find(destination) == subscriptions.end()) {
        std::cerr << "Error: Channel " << destination << " not found" << std::endl;
        return "";
    }
    std::ostringstream frame;
    frame << "SEND" << "\n"
          << "destination:/" << destination << "\n\n"
          << "user: "<< username << '\n'
          << body << '\n'
          << '\0';
    return frame.str();
}

std::string StompProtocol::createDisconnectFrame(int receipt) {
    std::ostringstream frame;
    frame << "DISCONNECT\n"
          << "receipt:" << receipt << "\n\n"
          << '\0';

    receipts[receipt] = "logout";
    return frame.str();
}

std::string StompProtocol::createSummary(const std::string& channel_name, const std::string& user, const std::vector<Event>& events) {
    std::ostringstream oss;
    oss << "Channel " << channel_name << "\n"
        << "Stats:\n"
        << "Total: " << events.size() << "\n";

    int activeCount = 0;
    int forcesArrivalCount = 0;
    for (const auto& event : events) {
        if (event.get_general_information().at("active") == "true") {
            activeCount++;
        }
        if (event.get_general_information().at("forces_arrival_at_scene") == "true") {
            forcesArrivalCount++;
        }
    }

    oss << "Active: " << activeCount << "\n"
        << "Forces Arrival: " << forcesArrivalCount << "\n"
        << "Event Reports:\n";

    for (const auto& event : events) {
        oss << "Report:\n"
            << "city: " << event.get_city() << "\n"
            << "date time: " << event.get_date_time() << "\n"
            << "event name: " << event.get_name() << "\n"
            << "summary: " << event.get_description().substr(0, 27) << (event.get_description().size() > 27 ? "..." : "") << "\n";
    }

    return oss.str();
}

void StompProtocol::processFrame(const std::string& frame) {
    std::cout << "[DEBUG] Processing frame: " << frame << std::endl;
    if (frame.find("CONNECTED") != std::string::npos) {
        connected = true;
        std::cout << "Login successful" << std::endl;

    } else if (frame.find("RECEIPT") != std::string::npos) {
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
        else {
            std::cout << "Receipt received for command: " << command << std::endl;
        }
            
        
    }   
}