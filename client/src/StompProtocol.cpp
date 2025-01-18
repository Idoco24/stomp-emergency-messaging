#include "../include/StompProtocol.h"
#include <sstream>
#include <iostream>

StompProtocol::StompProtocol(ConnectionHandler& connectionHandler) 
    : connectionHandler(connectionHandler), connected(false), subscriptions(), gameEvents() {}

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
    return frame.str();
}

std::string StompProtocol::createUnsubscribeFrame(int id, int receipt) {
    std::ostringstream frame;
    frame << "UNSUBSCRIBE\n"
          << "id:" << id << "\n"
          << "receipt:" << receipt << "\n\n"
          << '\0';
    return frame.str();
}

std::string StompProtocol::createSendFrame(const std::string& destination, const std::string& body) {
    std::ostringstream frame;
    frame << "SEND" << "\n"
          << "destination:" << destination << "\n\n"
          << body << '\n'
          << '\0';
    return frame.str();
}

std::string StompProtocol::createDisconnectFrame(int receipt) {
    std::ostringstream frame;
    frame << "DISCONNECT\n"
          << "receipt:" << receipt << "\n\n"
          << '\0';
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

}