#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <sstream>
#include <thread>
#include "../include/ConnectionHandler.h"
#include "../include/StompProtocol.h"
#include "../include/SafeQueue.h"

// The terminal thread only enqueues text. All socket/protocol state belongs to main.
int main() {
    SafeQueue<std::string> commands;
    std::atomic<bool> inputDone(false);
    std::thread keyboard([&]() {
        std::string line;
        while (std::getline(std::cin, line)) {
            commands.enqueue(line);
            if (line == "quit") break;
        }
        inputDone = true;
    });
    std::unique_ptr<ConnectionHandler> handler;
    std::unique_ptr<StompProtocol> protocol;
    int receipt = 1, subscription = 1;
    bool running = true;
    auto send = [&](std::string frame) {
        if (!frame.empty() && handler && !handler->sendLine(frame)) handler->close();
    };
    while (running) {
        if (!commands.isEmpty()) {
            std::string line = commands.dequeue();
            std::istringstream args(line);
            std::string command; args >> command;
            try {
                if (command == "quit") { running = false; continue; }
                if (command == "login") {
                    if (handler && handler->isConnected()) { std::cout << "Already connected" << std::endl; continue; }
                    std::string address, user, password; args >> address >> user >> password;
                    auto colon = address.rfind(':');
                    if (colon == std::string::npos || user.empty() || password.empty())
                        throw std::invalid_argument("Usage: login host:port username password");
                    size_t consumed = 0;
                    int port = std::stoi(address.substr(colon + 1), &consumed);
                    if (port < 1 || port > 65535 || consumed != address.size() - colon - 1)
                        throw std::invalid_argument("Port must be between 1 and 65535");
                    handler.reset(new ConnectionHandler(address.substr(0, colon), static_cast<unsigned short>(port)));
                    protocol.reset(new StompProtocol(*handler));
                    if (handler->connect()) send(protocol->createConnectFrame("stomp.cs.bgu.ac.il", user, password));
                    else { protocol.reset(); handler.reset(); }
                } else if (!protocol || !protocol->isConnected()) {
                    if (!command.empty()) std::cout << "Please login first" << std::endl;
                } else if (command == "join") {
                    std::string channel; args >> channel;
                    if (channel.empty()) throw std::invalid_argument("Usage: join channel");
                    send(protocol->createSubscribeFrame(channel, subscription++, receipt++));
                } else if (command == "exit") {
                    std::string channel; args >> channel;
                    if (channel.empty()) throw std::invalid_argument("Usage: exit channel");
                    send(protocol->createUnsubscribeFrame(channel, receipt++));
                } else if (command == "report") {
                    std::string file; args >> file;
                    if (file.empty()) throw std::invalid_argument("Usage: report file.json");
                    names_and_events parsed = parseEventsFile(file);
                    if (!protocol->isSubscribed(parsed.channel_name)) throw std::invalid_argument("Join the report channel first");
                    // The username for the active session is retained below after CONNECT.
                    for (Event event : parsed.events) {
                        event.setEventOwnerUser(protocol->getUsername());
                        send(protocol->createSendFrame(event));
                    }
                    std::cout << "reported" << std::endl;
                } else if (command == "summary") {
                    std::string channel, user, file; args >> channel >> user >> file;
                    if (channel.empty() || user.empty() || file.empty()) throw std::invalid_argument("Usage: summary channel user file");
                    protocol->createSummary(channel, user, protocol->getEvents(channel, user), file);
                    std::cout << "Summary written to " << file << std::endl;
                } else if (command == "logout") {
                    send(protocol->createDisconnectFrame(receipt++));
                } else if (!command.empty()) std::cout << "Unknown command" << std::endl;
            } catch (const std::exception& error) {
                std::cerr << "Command error: " << error.what() << std::endl;
            }
        }
        if (handler && handler->isConnected() && protocol) {
            std::string frame;
            if (handler->getFrameAscii(frame, '\0')) {
                try { protocol->processFrame(frame); }
                catch (const std::exception& error) { std::cerr << "Protocol error: " << error.what() << std::endl; handler->close(); }
            }
        }
        if (handler && !handler->isConnected()) { protocol.reset(); handler.reset(); }
        if (inputDone && commands.isEmpty()) running = false;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    if (handler) handler->close();
    keyboard.join();
    return 0;
}
