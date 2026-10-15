CXX ?= g++
CXXFLAGS ?= -std=c++17 -O2 -Wall -Wextra -pthread
CPPFLAGS += -Iinclude
LDLIBS += -lboost_system -pthread
SOURCES = src/ConnectionHandler.cpp src/event.cpp src/StompClient.cpp src/StompProtocol.cpp
OBJECTS = $(SOURCES:src/%.cpp=bin/%.o)

all: bin/StompEMIClient

bin/StompEMIClient: $(OBJECTS)
	$(CXX) $(CXXFLAGS) $^ $(LDLIBS) -o $@

bin/%.o: src/%.cpp | bin
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -MMD -MP -c $< -o $@

bin:
	mkdir -p bin

run: all
	./bin/StompEMIClient

clean:
	rm -f bin/*.o bin/*.d bin/StompEMIClient

-include $(OBJECTS:.o=.d)
.PHONY: all run clean
