package bgu.spl.net.impl.stomp;

import bgu.spl.net.srv.Server;




public class StompServer {
    
    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("you must supply two arguments: <port>, <type_of_server> - tpc / reactor");
            System.exit(1);
        }

        String server_type = args[1];

        if (!server_type.equals("tpc") && !server_type.equals("reactor")) {
            System.out.println("you must supply on the second argument: <type_of_server> - tpc / reactor");
            System.exit(1);
        }

        int port = Integer.parseInt(args[0]);
        String serverType = args[1];
       if (serverType.equals("tpc")){
           Server.threadPerClient(port, StompProtocolImpl::new, StompMessageEncodeDecoderImpl::new).serve();
        
       } else if (serverType.equals("reactor")){
           Server.reactor(4, port, StompProtocolImpl::new, StompMessageEncodeDecoderImpl::new).serve();
       
        } else {
           System.out.println("Unknown server type");
           System.exit(1);
       }
    }
}
