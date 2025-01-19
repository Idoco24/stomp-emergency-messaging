#include <iostream>
#include <string>
#include <thread>
#include <mutex>
#include <fstream>
#include "../include/ConnectionHandler.h"
#include "../include/StompProtocol.h"
#include "../include/keyboardInput.h"
#include "../include/SafeQueue.h"

ConnectionHandler* login(const std::string& host_port, const std::string& username, const std::string& password);

int main(int argc, char *argv[]) {
    SafeQueue<std::string> messageQueue;
    bool shouldRun = true;
    std::shared_ptr<ConnectionHandler> handler = nullptr;
    std::shared_ptr<StompProtocol> stompProtocol = nullptr;
    std::mutex handlerMutex;

    // Keyboard input thread
    std::thread keyboardThread([&messageQueue, &shouldRun, &handler, &stompProtocol, &handlerMutex]() {
        int sub = 0;
        int id = 0;
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
                        
                        newHandler->sendLine(connectFrame);

                        std::string response;
                        std::string fullresponse;
                        while (newHandler->getLine(response) && response != "") {
                            fullresponse += response;
                        }
                        if (fullresponse.find("CONNECTED") != std::string::npos) {
                            // std::cout << "Login successful" << std::endl;
                            std::lock_guard<std::mutex> lock(handlerMutex);
                            
                            handler = newHandler;
                            stompProtocol = newStompProtocol;
                            stompProtocol->processFrame(fullresponse);
                            log = true;
                        } else {
                            std::cout << "Error: " << fullresponse << std::endl;
                        }
                    }
                }
            } else {
                std::lock_guard<std::mutex> lock(handlerMutex);
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
                        messageQueue.enqueue(stompProtocol->createSubscribeFrame(args, id++, sub++));
                    }
                } else if (line == "logout") {
                    messageQueue.enqueue(stompProtocol->createDisconnectFrame(sub++));
                    // std::cout << "Logged out" << std::endl;
                    // handler = nullptr;
                    // stompProtocol = nullptr;
                    // log = false;
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                
                }else if (line.find("exit") != std::string::npos) {
                    std::istringstream iss(line);
                    std::string command, args;
                    iss >> command >> args;
                    if (args.empty()) {
                        std::cout << "Usage: exit {channel name}" << std::endl;
                    } else {
                        messageQueue.enqueue(stompProtocol->createUnsubscribeFrame(args, sub++));
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
                        std::cout << "channel name: " << channel_name << std::endl;
                        std::vector<Event> events = mappy.events;
                        
                        for (Event event: events){
                            std::string report = stompProtocol->createSendFrame(channel_name, user ,event.toString());
                            if (report!= ""){
                            messageQueue.enqueue(report);
                        }}
                        // std::string report = stompProtocol->createSendFrame()
                    }
                }
                 else {
                    std::cout << "Unknown command" << std::endl;
                }
            }
        }
    });

    // Socket communication thread
    std::thread socketThread([&handler, &stompProtocol, &messageQueue, &shouldRun, &handlerMutex]() {
        while (shouldRun) {
            std::shared_ptr<ConnectionHandler> currentHandler;
            std::shared_ptr<StompProtocol> currentStompProtocol;

            {
                std::lock_guard<std::mutex> lock(handlerMutex);
                currentHandler = handler;
                currentStompProtocol = stompProtocol;
            }

            if (currentHandler && currentHandler->isConnected()) {
                // while (!messageQueue.empty()) {
                    std::string message = messageQueue.dequeue();
                    currentHandler->sendLine(message);
                

                    std::string response;
                    std::string fullresponse;
                    while (currentHandler->getLine(response) && response != "") {
                        fullresponse += response;
                        response.clear();
                    }
                    if (currentStompProtocol) {
                        currentStompProtocol->processFrame(fullresponse);
                }
                // }
            } else {
                // std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }
        std::cout << "[DEBUG] Exiting socket thread" << std::endl;
    });

    keyboardThread.join();
    socketThread.join();

    return 0;
}