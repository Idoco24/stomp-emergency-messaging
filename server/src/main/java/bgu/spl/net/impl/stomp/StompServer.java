package bgu.spl.net.impl.stomp;

import bgu.spl.net.srv.Server;




public class StompServer {
    
    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage: StompServer <port> <reactor/tpc>");
            System.exit(1);
        }
        int port = Integer.parseInt(args[0]);
        String serverType = args[1];
        System.out.println("Server port: " + port);
        System.out.println("Server type: " + serverType);
//        if (serverType.equals("tpc")){
//            Server.threadPerClient(port, StompMessagingProtocol::new, StompMessageEncoderDecoder::new).serve();
//        } else if (serverType.equals("reactor")){
//            Server.reactor(Runtime.getRuntime().availableProcessors(), port, StompMessagingProtocol::new, StompMessageEncoderDecoder::new).serve();
//        } else {
//            System.out.println("Unknown server type");
//            System.exit(1);
//        }
    }
}
