#include "../include/ConnectionHandler.h"

using boost::asio::ip::tcp;

using std::cin;
using std::cout;
using std::cerr;
using std::endl;
using std::string;

ConnectionHandler::ConnectionHandler(string host, short port) : host_(host), port_(port), io_service_(),
                                                                socket_(io_service_) {}

ConnectionHandler::~ConnectionHandler() {
    close();
}

bool ConnectionHandler::connect() {
    std::cout << "Starting connect to " << host_ << ":" << port_ << std::endl;
    try {
        tcp::endpoint endpoint(boost::asio::ip::address::from_string(host_), port_);
        boost::system::error_code error;

        // Establish connection
        socket_.connect(endpoint, error);
        if (error) throw boost::system::system_error(error);

        // Set the socket to non-blocking mode
        socket_.non_blocking(true);
    } catch (std::exception &e) {
        std::cerr << "Connection failed (Error: " << e.what() << ')' << std::endl;
        return false;
    }
    return true;
}

bool ConnectionHandler::getBytes(char bytes[], unsigned int bytesToRead) {
    size_t tmp = 0;
    boost::system::error_code error;
    auto start = std::chrono::steady_clock::now();
    const auto timeout = std::chrono::seconds(1); // Set a timeout duration

    try {
        while (bytesToRead > tmp) {
            size_t bytesRead = socket_.read_some(boost::asio::buffer(bytes + tmp, bytesToRead - tmp), error);

            if (error == boost::asio::error::would_block) {
                // No data available, retry later
                std::this_thread::sleep_for(std::chrono::milliseconds(10));

                // Check for timeout
                auto now = std::chrono::steady_clock::now();
                if (now - start > timeout) {
                    return false;
                }
                continue;
            } else if (error) {
                std::cerr << "Error reading bytes: " << error.message() << std::endl; // Debug statement
                throw boost::system::system_error(error);
            }

            tmp += bytesRead;
            // std::cout << "Bytes read: " << bytesRead << ", Total bytes read: " << tmp << std::endl; // Debug statement
        }
    } catch (std::exception &e) {
        std::cerr << "recv failed (Error: " << e.what() << ')' << std::endl;
        return false;
    }

    return true;
}

bool ConnectionHandler::sendBytes(const char bytes[], int bytesToWrite) {
    int tmp = 0;
    boost::system::error_code error;
    try {
        while (!error && bytesToWrite > tmp) {
            tmp += socket_.write_some(boost::asio::buffer(bytes + tmp, bytesToWrite - tmp), error);
        }
        if (error)
            throw boost::system::system_error(error);
    } catch (std::exception &e) {
        std::cerr << "send failed (Error: " << e.what() << ')' << std::endl;
        return false;
    }
    //std::cout << "Sent bytes: " << std::string(bytes, bytesToWrite) << std::endl; // Debug print
    return true;
}

bool ConnectionHandler::getLine(std::string &line) {
    //std::cout << "Attempting to get line" << std::endl;
    // return getFrameAscii(line, '\0');
    return getFrameAscii(line, '\n');
}

bool ConnectionHandler::sendLine(std::string &line) {
    //std::cout << "Attempting to send line: " << line << std::endl;
    return sendFrameAscii(line, '\0');
    // return sendFrameAscii(line, '\n');
}

bool ConnectionHandler::getFrameAscii(std::string &frame, char delimiter) {
    char ch;
    auto start = std::chrono::steady_clock::now();
    const auto timeout = std::chrono::seconds(1); // Set a timeout duration
    try {
        do {
            if (!getBytes(&ch, 1)) {				
                return false;
            }
            if (ch == '\0') {
                // std::cout << "Null character received, ending frame" << std::endl;
                return true;
            }

            frame.append(1, ch);
            // std::cout << "Received character: " << ch << std::endl;

            // Check for timeout
            auto now = std::chrono::steady_clock::now();
            if (now - start > timeout) {
                return false;
            }
        } while (delimiter != ch);

        // std::cout << "Delimiter received, frame complete" << std::endl;
    } catch (std::exception &e) {
        std::cerr << "recv failed2 (Error: " << e.what() << ')' << std::endl;
        return false;
    }
    return true;
}

bool ConnectionHandler::sendFrameAscii(const std::string &frame, char delimiter) {
    //std::cout << "Sending frame: " << frame << std::endl;
    bool result = sendBytes(frame.c_str(), frame.length());
    if (!result) return false;
    return sendBytes(&delimiter, 1);
    // return result;
}

// Close down the connection properly.
void ConnectionHandler::close() {
    try {
        socket_.close();
    } catch (...) {
        std::cout << "closing failed: connection already closed" << std::endl;
    }
}

bool ConnectionHandler::isConnected() {
    return socket_.is_open();
}