import java.io.*;
import java.net.*;
import java.util.*;


public class Client {
    private DatagramSocket udpSocket;
    private Socket tcpSocket;
    private DataInputStream tcpInputStream;
    private DataOutputStream tcpOutputStream;
    private int targetTCP;
    private int targetUDP;
    private int clientUDP;
    private String host;
    private boolean isTCP;


    public Client(){}

    public Client(String host, int tcpPort, int udpPort, boolean isTCP) throws IOException {
        this.host = host;
        this.targetTCP = tcpPort;
        this.targetUDP = udpPort;
        this.isTCP = isTCP;
        tcpSocket = new Socket(host, tcpPort);
        tcpInputStream = new DataInputStream(tcpSocket.getInputStream());
        tcpOutputStream = new DataOutputStream(tcpSocket.getOutputStream());
        ServerSocket serverSocket = new ServerSocket(0);
        clientUDP = serverSocket.getLocalPort();
        udpSocket = new DatagramSocket(clientUDP);
    }

    public String getHost() {
        return host;
    }

    public int getTargetUDP() {
        return targetUDP;
    }

    public void setTCP(boolean TCP) {
        isTCP = TCP;
    }

    public DataInputStream getTcpInputStream() {
        return tcpInputStream;
    }

    public DataOutputStream getTcpOutputStream() {
        return tcpOutputStream;
    }

    public DatagramSocket getUdpSocket() {
        return udpSocket;
    }

    public static String readMultipleMsgTCP(Client client) throws IOException {
        String result = "";
        while(!result.contains("<EOM>")) {
            byte[] receiveFromServerData = new byte[1024];
            client.getTcpInputStream().read(receiveFromServerData);
            String receiveFromServer = new String(receiveFromServerData).replaceAll("\\u0000", "");
            result = result + receiveFromServer;
        }
        result = result.replace("<EOM>", "");
        return result;
    }

    public static void sendMsgTCP(String st, Client client) throws IOException {
        client.getTcpOutputStream().writeBytes(st + "\n");
        //client.getTcpOutputStream().close();
    }

    public static void sendMsgUDP(String st, Client client) throws IOException {
        byte[] sendData = st.getBytes();
        InetAddress address = InetAddress.getByName(client.getHost());
        int port = client.getTargetUDP();
        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, address, port);
        client.getUdpSocket().send(sendPacket);
    }

    public static String readMsgUDP(Client client){
        byte[] receiveData = new byte[1500];
        DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
        try {
            client.getUdpSocket().setSoTimeout(1*1000);
            client.getUdpSocket().receive(receivePacket);
        } catch (Exception e) {
        }
        return new String(receivePacket.getData()).trim();
    }

    public static void sendMsg(String st, Client client) throws IOException {
        if(client.isTCP){
            sendMsgTCP(st, client);
        }else{
            sendMsgUDP(st, client);
        }
    }

    public static String readMsg(Client client) throws IOException {
        if(client.isTCP){
            return readMultipleMsgTCP(client);
        }else{
            return readMsgUDP(client);
        }
    }

    public static void main(String args[]) throws IOException {
        String hostAddress ;
        int tcpPort ;
        int udpPort ;

        if (args.length != 3) {
            System.out.println("ERROR: Provide 3 arguments");
            System.out.println("\t(1) <hostAddress>: the address of the server");
            System.out.println("\t(2) <tcpPort>: the port number for TCP connection");
            System.out.println("\t(3) <udpPort>: the port number for UDP connection");
            System.exit(-1);
        }

        hostAddress = args[0];
        tcpPort = Integer.parseInt(args[1]);
        udpPort = Integer.parseInt(args[2]);

        //start client
        Client client = new Client(hostAddress, tcpPort, udpPort, true);
        //
        Scanner sc = new Scanner(System.in);
        while(sc.hasNextLine()) {
            String cmd = sc.nextLine();
            String[] tokens = cmd.split(" ");

            if (tokens[0].equals("setmode")) {
                if(tokens[1].equals("T")){
                    client.setTCP(true);
                    System.out.println("we are using TCP");
                }else if(tokens[1].equals("U")){
                    client.setTCP(false);
                    System.out.println("we are using UDP");
                }else{
                    System.out.println("ERROR: No such command");
                }
            }
            else if (tokens[0].equals("purchase")) {
                sendMsg(cmd,client);
                String purchase = readMsg(client);
                System.out.println(purchase);

            } else if (tokens[0].equals("cancel")) {
                sendMsg(cmd,client);
                String cancel = readMsg(client);
                System.out.println(cancel);
            } else if (tokens[0].equals("search")) {
                sendMsg(cmd,client);
                String search = readMsg(client);
                System.out.println(search);
            } else if (tokens[0].equals("list")) {
                sendMsg(cmd,client);
                String list = readMsg(client);
                System.out.println(list);
            } else {
                System.out.println("ERROR: No such command");
            }
        }

    }

}
