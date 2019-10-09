import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.IOException;
import java.lang.InterruptedException;
import java.lang.NumberFormatException;
import java.lang.StringBuilder;
import java.lang.Thread;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Map;
import java.util.TreeMap;
import java.util.Scanner;


// Specifies the commands the server will support.
interface IServer {
  String executeCommand(String command);
}

public class Server implements IServer {

  // Data interface
  class DataInterface {

    private Map<String, Integer> nameToSeatNumMap = new HashMap<>();

    public DataInterface() { }

    // reserve
    public synchronized String reserve(String name) {
      return "";
    }

    // bookSeat
    public synchronized String bookSeat(String name, int seatNum) {
      return "";
    }

    // search
    public synchronized String search(String name) {
      return "";
    }

    // delete
    public synchronized String delete(String name) {
      return "";
    } 

  }

  private DataInterface dataInterface = new DataInterface();


  // TCP Command Client Thread
  class TcpCommandClientThread extends Thread {

    private int clientId;
    private ITcpServerThread tcpServerThread;
    private IServer server;
    private Socket socket;
    private boolean running;


    public TcpCommandClientThread(int clientId, Socket socket, ITcpServerThread tcpServerThread, IServer server) {
      this.clientId = clientId;
      this.socket = socket;
      this.tcpServerThread = tcpServerThread;
      this.server = server;
    }

    public void run() {
      try {
        InetAddress address = socket.getInetAddress();
        int port = socket.getPort();
        System.out.println("TCP client connected from " + address.toString() + ":" + port);
        running = true;

        BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        DataOutputStream dos = new DataOutputStream(socket.getOutputStream());

        try {
        while (running) {

          // Read command.
          String command = br.readLine();
          if (command == null) {
            running = false;
            break;
          }

          String response = server.executeCommand(command);
          if (response == null) { response = "Did not understand command " + command; }
          response += "<EOM>";
          dos.writeBytes(response);
          dos.flush();
        }
          tcpServerThread.notifyClientDisconnected(clientId, socket);
        } catch (SocketException e) {
          tcpServerThread.notifyClientDisconnected(clientId, socket);
        }
      } catch (Exception e) {
        System.out.println("Error in TcpCommandClientThread");
        e.printStackTrace();
      }
    }

    @Override
    public void interrupt() {
      super.interrupt();
      running = false;
      try {
        socket.close();
      } catch (IOException e) {
        System.out.println("Unable to close TCP Client socket.");
        e.printStackTrace();
      }
    }
  }

  interface ITcpServerThread {
    void notifyClientDisconnected(int clientId, Socket socket);
  }

  // TCP Server Thread
  class TcpServerThread extends Thread implements ITcpServerThread {

    private ServerSocket socket;
    private IServer server;
    private int port;
    private boolean running;
    private int clientIdCounter = 1;
    private Map<Integer, TcpCommandClientThread> clientIdToClientThreadMap = new TreeMap<>();
   

    public TcpServerThread(int port, IServer server) {
      this.port = port;
      this.server = server;
    }

    public void run() {
      try {
        running = true;
        socket = new ServerSocket(port);
        System.out.println("TCP server started. Listening on " + socket.getLocalSocketAddress());
        while (running) {
          Socket clientSocket = socket.accept();
          TcpCommandClientThread tcpClientThread = new TcpCommandClientThread(clientIdCounter, clientSocket, this, server);
          clientIdToClientThreadMap.put(clientIdCounter, tcpClientThread);
          tcpClientThread.start();
          clientIdCounter++;
        }
      } catch (Exception e) {
        System.out.println("Error in TcpServerThread.");
        e.printStackTrace();
      }
    }

    @Override
    public synchronized void notifyClientDisconnected(int clientId, Socket socket) {
      InetAddress address = socket.getInetAddress();
      int port = socket.getPort();
      System.out.println("TCP client disconnected from " + address.toString() + ":" + port);
      clientIdToClientThreadMap.remove(clientId);
    }

    @Override
    public void interrupt() {
      super.interrupt();
      running = false;
      try {
        socket.close();
      } catch (IOException e) {
        System.out.println("Unable to close TCP Server socket.");
        e.printStackTrace();
      }
    }


  }

  public Server() { }

  // Parses String to an int.
  private int parseInt(String value) {
    int retVal;
    try {
      retVal = Integer.parseInt(value);
    } catch (NumberFormatException e) {
      retVal = 0;
    }
    return retVal;
  }

  // Executes command from client and returns a response. Returns null if an unknown command is provided.
  public String executeCommand(String command) {
    String[] tokens = command.split(" ");
    if (tokens[0].equals("reserve") && tokens.length == 2) {
      String name = tokens[1];
      return dataInterface.reserve(name);
    } else if (tokens[0].equals("bookSeat") && tokens.length == 3) {
      String name = tokens[1];
      int seatNum = parseInt(tokens[2]);
      return dataInterface.bookSeat(name, seatNum);
    } else if (tokens[0].equals("search") && tokens.length == 2) {
      String name = tokens[1];
      return dataInterface.search(name);
    } else if (tokens[0].equals("delete") && tokens.length == 2) {
      String name = tokens[1];
      return dataInterface.delete(name);
    } else {
      return null;
    }
  }

  public void start(int myID, int numServer, int numSeat, List<String> replicaHostStrings) {

    try {

      int numReplicaConnectionsRemaining = numServer;

      // Start the TCP clients to the replica servers.
      for (String replicaHostString : replicaHostStrings) {
        System.out.println("Waiting on connection acceptance from " + replicaHostString + ". [" + numReplicaConnectionsRemaining + " replica connections remaining before accepting client commands].");
        numReplicaConnectionsRemaining--;
      }

      // Start the TCP server.
      //TcpServerThread tcpServerThread = new TcpServerThread(tcpPort, this);
      //tcpServerThread.start();

      // Wait for the threads to finish.
      //tcpServerThread.join();

    //} catch (InterruptedException e) {
    //  System.out.println("Main thread interrupted.");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void main (String[] args) {

    Scanner sc = new Scanner(System.in);
    int myID = sc.nextInt();
    int numServer = sc.nextInt();
    int numSeat = sc.nextInt();

    List<String> replicaHostStrings = new ArrayList<>();
    
    for (int i = 0; i < numServer; i++) {
      replicaHostStrings.add(sc.next());
    }

    System.out.println("");
    System.out.println("###############################################");
    System.out.println("SERVER " + myID + " is starting!");
    System.out.println("Number of replica servers: " + numServer);
    System.out.println("Number of seats: " + numSeat);
    for (String replicaHostString : replicaHostStrings) {
      System.out.println("Replica Server: " + replicaHostString);
    }
    System.out.println("###############################################");

    Server server = new Server();
    server.start(myID, numServer, numSeat, replicaHostStrings);

  }
}
