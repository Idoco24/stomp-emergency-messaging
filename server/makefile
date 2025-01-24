all: clean compile run

clean:
	mvn clean

compile:
	mvn compile

run:
	clear
	mvn exec:java -Dexec.mainClass="bgu.spl.net.impl.stomp.StompServer" -Dexec.args="7777 reactor"
