#include "../include/ConnectionHandler.h"
#include <chrono>
#include <thread>

ConnectionHandler::ConnectionHandler(std::string host, unsigned short port)
    : host_(host), port_(port), io_service_(), socket_(io_service_), pendingFrame_() { }
ConnectionHandler::~ConnectionHandler() { close(); }

bool ConnectionHandler::connect() {
    try {
        tcp::resolver resolver(io_service_);
        boost::asio::connect(socket_, resolver.resolve(host_, std::to_string(port_)));
        socket_.non_blocking(true);
        return true;
    } catch (const std::exception& error) {
        std::cerr << "Connection failed: " << error.what() << std::endl;
        close(); return false;
    }
}

bool ConnectionHandler::getBytes(char bytes[], unsigned int count) {
    unsigned int received = 0;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(50);
    while (received < count && socket_.is_open()) {
        boost::system::error_code error;
        size_t amount = socket_.read_some(boost::asio::buffer(bytes + received, count - received), error);
        if (error == boost::asio::error::would_block || error == boost::asio::error::try_again) {
            if (std::chrono::steady_clock::now() >= deadline) return false;
            std::this_thread::sleep_for(std::chrono::milliseconds(2)); continue;
        }
        if (error || amount == 0) { close(); return false; }
        received += static_cast<unsigned int>(amount);
    }
    return received == count;
}

bool ConnectionHandler::sendBytes(const char bytes[], int count) {
    int sent = 0;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (sent < count && socket_.is_open()) {
        boost::system::error_code error;
        size_t amount = socket_.write_some(boost::asio::buffer(bytes + sent, count - sent), error);
        if (error == boost::asio::error::would_block || error == boost::asio::error::try_again) {
            if (std::chrono::steady_clock::now() >= deadline) { close(); return false; }
            std::this_thread::sleep_for(std::chrono::milliseconds(2)); continue;
        }
        if (error || amount == 0) { close(); return false; }
        sent += static_cast<int>(amount);
    }
    return sent == count;
}

bool ConnectionHandler::getLine(std::string& line) { return getFrameAscii(line, '\n'); }
bool ConnectionHandler::sendLine(std::string& line) { return sendFrameAscii(line, '\0'); }
bool ConnectionHandler::getFrameAscii(std::string& frame, char delimiter) {
    char next;
    while (getBytes(&next, 1)) {
        if (next == delimiter || next == '\0') {
            frame.swap(pendingFrame_); pendingFrame_.clear(); return true;
        }
        pendingFrame_.push_back(next);
    }
    // Keep partial data between polling calls; TCP may split a frame at any byte.
    return false;
}
bool ConnectionHandler::sendFrameAscii(const std::string& frame, char delimiter) {
    return sendBytes(frame.data(), static_cast<int>(frame.size())) && sendBytes(&delimiter, 1);
}
void ConnectionHandler::close() {
    boost::system::error_code ignored;
    socket_.shutdown(tcp::socket::shutdown_both, ignored);
    socket_.close(ignored);
}
bool ConnectionHandler::isConnected() { return socket_.is_open(); }
