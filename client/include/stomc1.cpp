#include <iostream>
#include <string>
#include <thread>
#include <mutex>
#include <fstream>
#include "../include/ConnectionHandler.h"
#include "../include/StompProtocol.h"
#include "../include/keyboardInput.h"
#include "../include/SafeQueue.h"

ConnectionHandler* login();

int main(int argc, char *argv[]) {
    ConnectionHandler* handler = nullptr;
    while (handler == nullptr) {
        handler = login();
    }

    // std::cout << "Logged in MADAFAKA!" << std::endl;

    StompProtocol stompProtocol(*handler);
    SafeQueue<std::string> messageQueue;
    bool shouldRun = true;
    // bool log = fasle;

    // Keyboard input thread
    std::thread keyboardThread([&messageQueue, &shouldRun, &stompProtocol]() {
        int sub = 1;
        int id = 1;
        
        while (shouldRun) {
            
            
            std::string line;
            std::getline(std::cin, line);
            
            //  if !log login() else all other commands

            if (line.find("join") != std::string::npos) {
                std::istringstream iss(line);
                std::string command, args;
                iss >> command >> args;
                if (args.empty()) {
                    std::cout << "Usage: join {channel name}" << std::endl;
                } else {
                    messageQueue.enqueue(stompProtocol.createSubscribeFrame(args, id++, sub++));
                    continue;
                }
            } else if (line == "logout") {
                messageQueue.enqueue(stompProtocol.createDisconnectFrame(sub++));
                std::cout << "Logged out" << std::endl;
                continue;
            } else if (line.find("login") != std::string::npos) {
                std::cout << "Already logged in, Please logout and try again." << std::endl;
                continue;
            } else {
                std::cout << "Unknown command" << std::endl;
            }
            
        }
    });

    // Socket communication thread
    std::thread socketThread([&handler, &stompProtocol, &messageQueue, &shouldRun]() {
        while (shouldRun) {
            if (handler->isConnected()) {
          
                // std::cout << "In socketThread" << std::endl;

                while (!messageQueue.empty()) {
                    std::string message = messageQueue.dequeue();
                    std::cout << message << std::endl;
                    messageQueue.enqueue(message); // Re-enqueue the message if needed
                }

                std::string message = messageQueue.dequeue();
                // std::cout << "Sending message: " << message << std::endl;
                handler->sendLine(message);
                
                std::string response;
                std::string fullresponse;
                while (handler->getLine(response) && response != "") {
                    // std::cout << "[DEBUG] Received response: " << response << std::endl;
                    fullresponse += response;
                    response.clear();
                }
                stompProtocol.processFrame(fullresponse);
                
                
                //  else {
                //     std::cerr << "[DEBUG] No response from server, exiting socket thread" << std::endl;
                //     shouldRun = false;
                // }
            }
        }
        std::cout << "[DEBUG] Exiting socket thread" << std::endl;
    });

    
    // while (shouldRun){
        keyboardThread.join();
        socketThread.join();
    // }
    delete handler;
    return 0;
}

ConnectionHandler* login() {
    while (true) {
        std::string line;
        std::getline(std::cin, line);
        std::istringstream iss(line);
        std::string command, host_port, username, password;
        iss >> command >> host_port >> username >> password;

        if (command == "login") {
            size_t colon_pos = host_port.find(':');
            if (colon_pos == std::string::npos) {
                std::cout << "Usage: login {host:port} {username} {password}" << std::endl;
                continue;
            }

            std::string host = host_port.substr(0, colon_pos);
            short port = std::stoi(host_port.substr(colon_pos + 1));

            ConnectionHandler* handler = new ConnectionHandler(host, port);
            if (handler->connect()) {
                StompProtocol stompProtocol(*handler);
                std::string connectFrame = stompProtocol.createConnectFrame("stomp.cs.bgu.ac.il", username, password);
                
                // std::cout << "\nSending connect frame: \n" << connectFrame << std::endl;
                handler->sendLine(connectFrame);

                std::string response;
                std::string fullresponse;
                while (handler->getLine(response) && response != "") {
                    fullresponse += response;
                    // std::cout << "Server response: " << response << std::endl;
                }
                // std::cout << "***\nFULL Server response!!!! * * * * * : \n" << fullresponse <<"\n***"<< std::endl;
                if (fullresponse.find("CONNECTED") != std::string::npos) {
                        std::cout << "Login successful" << std::endl;
                        return handler;
                    } else {
                        std::cout << "Error: " << fullresponse << std::endl;
                        delete handler;
                        continue;
                    }
            }
            delete handler;
        } else {
            std::cout << "Please login first" << std::endl;
        }
    }
}






/// TODO:
/*
• Handle logout command for user.
• Implement all commands.
• Implement proccessFrame for each frame.
• Add Reciept check for the proccess to know which command to solve.

• Final check for client side.
*/