#include <iostream>
#include <string>
#include <thread>
#include <mutex>
#include <fstream>
#include "../include/ConnectionHandler.h"
#include "../include/StompProtocol.h"
#include "../include/SafeQueue.h"

ConnectionHandler* login(const std::string& host_port, const std::string& username, const std::string& password);
void proccessAllQueuedMessages(SafeQueue<std::string >& queue, ConnectionHandler& handler);

int main(int argc, char *argv[]) {
    SafeQueue<std::string> messageQueue;
    bool shouldRun = true;
    std::shared_ptr<ConnectionHandler> handler = nullptr;
    std::shared_ptr<StompProtocol> stompProtocol = nullptr;
    std::mutex handlerMutex;

    // Keyboard input thread
    std::thread keyboardThread([&messageQueue, &shouldRun, &handler, &stompProtocol, &handlerMutex]() {
        int receipt = 0;
        int id = 1;
        bool log;
        std::string user;
        
        while (shouldRun) {
            if (stompProtocol==nullptr){
                // std::cout << "Please login first" << std::endl;
                log = false;
            }
            else {
                log = stompProtocol->isConnected();
            }

            std::string line;
            std::getline(std::cin, line);

            if (line.find("login") != std::string::npos) {
                if (log){
                    std::cout << "Already logged in, Please logout and try again." << std::endl;
                    continue;
                }
                std::istringstream iss(line);
                std::string command, host_port, username, password;
                iss >> command >> host_port >> username >> password;
                user = username;
                if (command == "login") {
                    size_t colon_pos = host_port.find(':');
                    if (colon_pos == std::string::npos) {
                        std::cout << "Usage: login {host:port} {username} {password}" << std::endl;
                        continue;
                    }

                    std::string host = host_port.substr(0, colon_pos);
                    short port = std::stoi(host_port.substr(colon_pos + 1));

                    std::shared_ptr<ConnectionHandler> newHandler = std::make_shared<ConnectionHandler>(host, port);
                    if (newHandler->connect()) {
                        std::shared_ptr<StompProtocol> newStompProtocol = std::make_shared<StompProtocol>(*newHandler);
                        std::string connectFrame = newStompProtocol->createConnectFrame("stomp.cs.bgu.ac.il", username, password);
                        // std::string connectFrame = newStompProtocol->createConnectFrame(host, username, password);
                        
                        newHandler->sendLine(connectFrame);

                        std::string response;
                        std::string fullresponse;
                        while (newHandler->getLine(response) && response != "") {
                            fullresponse += response;
                            response.clear();
                        }
                        if (fullresponse.find("CONNECTED") != std::string::npos) {
                            // std::cout << "Login successful" << std::endl;
                            
                            std::lock_guard<std::mutex> lock(handlerMutex); // 
                            
                            handler = newHandler;
                            stompProtocol = newStompProtocol;
                            stompProtocol->processFrame(fullresponse);
                            log = true;
                        } else {
                            stompProtocol->processFrame(fullresponse);
                        }
                    }
                }
            } else {
                // std::lock_guard<std::mutex> lock(handlerMutex);
                if (!log) {
                    std::cout << "Please login first" << std::endl;
                    continue;
                }

                if (line.find("join") != std::string::npos) {
                    std::istringstream iss(line);
                    std::string command, args;
                    iss >> command >> args;
                    if (args.empty()) {
                        std::cout << "Usage: join {channel name}" << std::endl;
                    } else {
                        messageQueue.enqueue(stompProtocol->createSubscribeFrame(args, id++, receipt++));
                    }
                } else if (line == "logout") {
                    messageQueue.enqueue(stompProtocol->createDisconnectFrame(receipt++));
                    // std::cout << "Logged out" << std::endl;
                    // handler = nullptr;
                    // stompProtocol = nullptr;
                    // log = false;
                    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                }else if (line.find("summary")!= std::string::npos){
                    std::istringstream iss(line);
                    std::string command, channel_name, user_send, outJson;
                    iss >> command >> channel_name >> user_send >> outJson;
                    if (channel_name.empty() || user_send.empty()) {
                        std::cout << "Usage: summary {channel name} {user} {file}" << std::endl;
                    }else{
                        std::vector<Event> eventus = stompProtocol->getEvents(channel_name, user_send);
                        stompProtocol->createSummary(channel_name, user_send, eventus, outJson);
                    }
                    
                }else if (line.find("exit") != std::string::npos) {
                    std::istringstream iss(line);
                    std::string command, args;
                    iss >> command >> args;
                    if (args.empty()) {
                        std::cout << "Usage: exit {channel name}" << std::endl;
                    } else {
                        messageQueue.enqueue(stompProtocol->createUnsubscribeFrame(args, receipt++));
                    }
                } else if (line.find("report")!= std::string::npos) {
                    std::istringstream iss(line);
                    std::string command, args;
                    iss >> command >> args;
                    if (args.empty()) {
                        std::cout << "Usage: report {file name}" << std::endl;
                    }else{
                        names_and_events mappy = parseEventsFile(args);
                        std::string channel_name = mappy.channel_name;
                        // std::cout << "channel name: " << channel_name << std::endl;
                        std::vector<Event> events = mappy.events;
                        // std::cout << "Channel sub id: " << stompProtocol->getSubscriptionId(channel_name) << std::endl;

                        if (!stompProtocol->isSubscribed(channel_name)){
                            std::cout << "You are not registerd to channel " << channel_name << std::endl;
                            continue;
                        }
                        else {
                            for (Event event: events){
                                event.setEventOwnerUser(user);
                                std::string report = stompProtocol->createSendFrame(event);
                                if (!report.empty()){
                                    messageQueue.enqueue(report);
                                }
                                
                            }
                            std::cout << "reported" << std::endl;
                        }
                        // std::string report = stompProtocol->createSendFrame()
                    }
                }
                 else {
                    std::cout << "Illegal command, please try a different one" << std::endl;
                }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));

            }
        }

    });

    // Socket communication thread
    std::thread socketThread([&handler, &stompProtocol, &messageQueue, &shouldRun, &handlerMutex]() {
        while (shouldRun) {
            // std::shared_ptr<ConnectionHandler> currentHandler;
            // std::shared_ptr<StompProtocol> currentStompProtocol;
            {
                // std::lock_guard<std::mutex> lock(handlerMutex);
                // currentHandler = handler;
                // currentStompProtocol = stompProtocol;
            }


            if (!handler || !stompProtocol) {
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                continue;
            }

            while (handler && handler->isConnected()) {
                if (!messageQueue.isEmpty()) {
                    std::string message = messageQueue.dequeue();
                    if (!message.empty()) {
                        // std::cout << "BEFORE Sending message "<< std::endl;
                        handler->sendLine(message);
                        // std::cout << "AFTER Sending message "<< std::endl;

                    }
                    continue;
                }

                std::string response;
                std::string fullresponse;
                
                std::lock_guard<std::mutex> lock(handlerMutex);
                // std::cout << "Socket" << std::endl;
                while (handler->getLine(response)&& response != "") {
                    fullresponse += response;
                    response.clear();
                }
                if (fullresponse.empty()) {
                    continue;
                }
                stompProtocol->processFrame(fullresponse);
                // std::cout << "BROCKET" << std::endl;
                // std::this_thread::sleep_for(std::chrono::milliseconds(100));
                
                // if (!messageQueue.isEmpty()) {
                // }
            } 

        // std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
        // std::cout << "[DEBUG] Exiting socket thread" << std::endl;

    });

    keyboardThread.join();
    socketThread.join();

    return 0;
}

void proccessAllQueuedMessages(SafeQueue<std::string >& queue, ConnectionHandler& handler){
    while (!queue.isEmpty()){
        std::string message = queue.dequeue();
        handler.sendLine(message);
    }
}