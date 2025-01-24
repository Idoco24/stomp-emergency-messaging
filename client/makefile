# CFLAGS:=-c -Wall -Weffc++ -g -std=c++11 -Iinclude
# LDFLAGS:=-lboost_system -lpthread
# # TODO change the makefile for StompEMIlient
# all: EchoClient

# EchoClient: bin/ConnectionHandler.o bin/echoClient.o
# 	g++ -o bin/EchoClient bin/ConnectionHandler.o bin/echoClient.o $(LDFLAGS)

# StompWCIClient: bin/ConnectionHandler.o bin/StompClient.obin/event.o
# 	g++ -o bin/StompWCIClient bin/ConnectionHandler.o bin/StompClient.o $(LDFLAGS)

# bin/ConnectionHandler.o: src/ConnectionHandler.cpp
# 	g++ $(CFLAGS) -o bin/ConnectionHandler.o src/ConnectionHandler.cpp

# bin/echoClient.o: src/echoClient.cpp
# 	g++ $(CFLAGS) -o bin/echoClient.o src/echoClient.cpp

# bin/event.o: src/event.cpp
# 	g++ $(CFLAGS) -o bin/event.o src/event.cpp

# bin/StompClient.o: src/StompClient.cpp
# 	g++ $(CFLAGS) -o bin/StompClient.o src/StompClient.cpp

# .PHONY: clean
# clean:
# 	rm -f bin/*

# # End of file

all : clean compile link run

clean:
	rm -f bin/*.o

compile: src/ConnectionHandler.cpp src/event.cpp src/StompClient.cpp src/StompProtocol.cpp
	g++ -c -Wall -Weffc++ -g -std=c++11 -Iinclude src/ConnectionHandler.cpp -o bin/ConnectionHandler.o
	g++ -c -Wall -Weffc++ -g -std=c++11 -Iinclude src/event.cpp -o bin/event.o
	g++ -c -Wall -Weffc++ -g -std=c++11 -Iinclude src/StompClient.cpp -o bin/StompClient.o
	g++ -c -Wall -Weffc++ -g -std=c++11 -Iinclude src/StompProtocol.cpp -o bin/StompProtocol.o

link:
	g++ -o bin/StompClient bin/ConnectionHandler.o bin/event.o bin/StompClient.o bin/StompProtocol.o -lpthread

run:
	clear
	./bin/StompClient


.PHONY: clean compile link run