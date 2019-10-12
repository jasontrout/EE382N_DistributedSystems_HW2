import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;


public class Client {
    private static int connectionTimeOut = 100;
    private static int receiveTimeOut = 5000;
    private Socket tcpSocket;
    private DataInputStream tcpInputStream;
    private DataOutputStream tcpOutputStream;
    private List<String> serverList;


    public Client(){}

    public Client(List<String> input) {
        serverList = input;
    }

    public List<String> getServerList() {
        return serverList;
    }

    public DataInputStream getTcpInputStream() {
        return tcpInputStream;
    }

    public DataOutputStream getTcpOutputStream() {
        return tcpOutputStream;
    }

    public Socket getTcpSocket() {
        return tcpSocket;
    }

    public void setTcpSocket(Socket tcpSocket) {
        this.tcpSocket = tcpSocket;
    }

    public void setTcpInputStream(DataInputStream tcpInputStream) {
        this.tcpInputStream = tcpInputStream;
    }

    public void setTcpOutputStream(DataOutputStream tcpOutputStream) {
        this.tcpOutputStream = tcpOutputStream;
    }

    public static void sendMsg(String st, Client client) {
        for(String address : client.getServerList()) {
            String host = address.split(":")[0];
            int port = Integer.valueOf(address.split(":")[1]);
            try {
                Socket socket = new Socket();
                socket.connect(new InetSocketAddress(host, port), connectionTimeOut);
                socket.setSoTimeout(receiveTimeOut);
                client.setTcpSocket(socket);
                client.setTcpInputStream(new DataInputStream(client.getTcpSocket().getInputStream()));
                client.setTcpOutputStream(new DataOutputStream(client.getTcpSocket().getOutputStream()));
                client.getTcpOutputStream().writeBytes(st + "\n");
                break;
            }catch (SocketTimeoutException e){
                System.out.println(String.format("after %s ms, Socket time out, can not connect with server, no message sent",Integer.toString(connectionTimeOut)));
            }catch (IOException e1){
                System.out.println(String.format("after %s ms, Socket time out, can not connect with server, no message sent",Integer.toString(connectionTimeOut)));
            }
        }
    }

    public static String readMsg(Client client) {
        String result = "";
        try {
            while(!result.contains("<EOM>")) {
                byte[] receiveFromServerData = new byte[1024];
                int i = client.getTcpInputStream().read(receiveFromServerData);
                String receiveFromServer = new String(receiveFromServerData).replaceAll("\\u0000", "");
                result += receiveFromServer;
            }
            result = result.replace("<EOM>", "");
        }catch (SocketTimeoutException e){
            System.out.println("Socket time out, can not connect with server, no message received");
            try {
                client.getTcpSocket().close();
            } catch (IOException e1) {
               // e1.printStackTrace();
            }
        }catch (IOException e2){
            System.out.println("IO Exception, can not connect with server");
        }

        return result;
    }

    public static String executeCmd(String cmd, Client client) throws IOException {
        sendMsg(cmd,client);
        String search = readMsg(client);
        if(search.length()==0){
            if(!client.getTcpSocket().isClosed()){
                client.getTcpSocket().close();
            }
            return "Disconnect with server, please input your command again";
        }else {
            return search;
        }
    }

    public static void main(String args[]) throws IOException {

        List<String> inputList = new ArrayList<>();
        Scanner sc = new Scanner(System.in);
        int numServer = sc.nextInt();
        sc.nextLine();
        for (int i = 0; i < numServer; i++) {
            String address = sc.nextLine();
            inputList.add(address);
        }

        //start client
        Client client = new Client(inputList);

        while(sc.hasNextLine()) {
            String cmd = sc.nextLine();
            String[] tokens = cmd.split(" ");

            if (tokens[0].equals("reserve")) {
                System.out.println(executeCmd(cmd, client));
            } else if (tokens[0].equals("bookSeat")) {
                System.out.println(executeCmd(cmd, client));
            } else if (tokens[0].equals("search")) {
                System.out.println(executeCmd(cmd, client));
            } else if (tokens[0].equals("delete")) {
                System.out.println(executeCmd(cmd, client));
            } else {
                System.out.println("ERROR: No such command");
            }
        }

    }

}
