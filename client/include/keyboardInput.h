#pragma once

#include <string>
#include <thread>
#include <mutex>
#include "../include/ConnectionHandler.h"
#include "../include/StompProtocol.h"

class KeyboardInput {
public:
    KeyboardInput(ConnectionHandler& connectionHandler, StompProtocol& stompProtocol);
    void run();

private:
    ConnectionHandler& connectionHandler;
    StompProtocol& stompProtocol;
    std::thread inputThread;
    std::mutex mtx;
    void handleInput();
};