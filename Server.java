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
import java.net.ConnectException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Map;
import java.util.PriorityQueue;
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

  // Comparator for sorting LamportClockEntry.
  class LamportClockEntryComparator implements Comparator<LamportClockEntry> {
    @Override
    public int compare(LamportClockEntry lce1, LamportClockEntry lce2) { 
      if (lce1.getTs() < lce2.getTs()) {
        return 1;
      } else if (lce1.getTs() > lce2.getTs()) {
        return -1;
      } else {
        if (lce1.getServerId() < lce2.getServerId()) {
          return 1;
        } else if (lce1.getServerId() > lce2.getServerId()) {
          return -1;
        } else {
          return 0;
        }
      }
    }
  }

  class LamportClockEntry {
    private int ts;
    private int serverId;
 
    public LamportClockEntry(int ts, int serverId) {
      this.ts = ts;
      this.serverId = serverId;
    }

    public int getTs() { return ts; }
    public int getServerId() { return serverId; }
  }

  private DataInterface dataInterface = new DataInterface();
  private PriorityQueue<LamportClockEntry> queue;
  private Map<Integer, TcpReplicaClient> serverIdToReplicaClientMap = new HashMap<>();
  private boolean serverLoaded;
  private int numServer;
  private int numServersConnected;
  private int serverId; // lamport clock server id
  private int ts; // lamport clock timestamp

  // TCP Replica CLient
  class TcpReplicaClient {
    private Socket socket;
    private IServer server;
    private DataOutputStream dos;
    public TcpReplicaClient(Socket socket, IServer server) {
      this.socket = socket;
      this.server = server;
      try {
        this.dos = new DataOutputStream(socket.getOutputStream());
      } catch (Exception ex) { 
        System.out.println("Unable to get data output stream");
        ex.printStackTrace();
      }
    }

    public String requestFullSync() {
      try {
        DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
        dos.writeBytes("requestFullSync\n");
        dos.flush();
      } catch (Exception ex) {
        System.out.println("Unable to request full sync");
        ex.printStackTrace();
      }
      return "";
    }

    public void requestLock() { 

    }

    public void ack() { 
    }

    public void syncSeatReserved(String name, int seatNum) { 

    }

    public void syncSeatUnreserved(int seatNum) {

    }

    public void close() { 
      try {
      } catch (Exception ex) { 
        System.out.println("Unable to close TcpReplicaClient");
        ex.printStackTrace();
      }
    }


  };

  // TCP Client Thread
  class TcpClientThread extends Thread {

    private int clientId;
    private ITcpServerThread tcpServerThread;
    private IServer server;
    private Socket socket;
    private boolean running;


    public TcpClientThread(int clientId, Socket socket, ITcpServerThread tcpServerThread, IServer server) {
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
        System.out.println("Error in TcpClientThread");
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
    private String hostname;
    private int port;
    private boolean running;
    private int clientIdCounter = 1;
    private Map<Integer, TcpClientThread> clientIdToClientThreadMap = new TreeMap<>();
   

    public TcpServerThread(String hostname, int port, IServer server) {
      this.hostname = hostname;
      this.port = port;
      this.server = server;
    }

    public void run() {
      try {
        running = true;
        socket = new ServerSocket(port, 100, InetAddress.getByName(hostname));
        System.out.println("TCP server started. Listening on " + socket.getLocalSocketAddress());
        while (running) {
          Socket clientSocket = socket.accept();
          TcpClientThread tcpClientThread = new TcpClientThread(clientIdCounter, clientSocket, this, server);
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

  public Server() { 
    queue = new PriorityQueue(11, new LamportClockEntryComparator());
  }

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
      if (!serverLoaded) { return "Server is not loaded."; }
      String name = tokens[1];
      return dataInterface.reserve(name);
    } else if (tokens[0].equals("bookSeat") && tokens.length == 3) {
      if (!serverLoaded) { return "Server is not loaded."; }
      String name = tokens[1];
      int seatNum = parseInt(tokens[2]);
      return dataInterface.bookSeat(name, seatNum);
    } else if (tokens[0].equals("search") && tokens.length == 2) {
      if (!serverLoaded) { return "Server is not loaded."; }
      String name = tokens[1];
      return dataInterface.search(name);
    } else if (tokens[0].equals("delete") && tokens.length == 2) {
      if (!serverLoaded) { return "Server is not loaded."; }
      String name = tokens[1];
      return dataInterface.delete(name);
    } else if (tokens[0].equals("requestFullSync") && tokens.length == 1) {
      System.out.println("Got requestFullSync");
      numServersConnected++;
      if (!serverLoaded && numServersConnected == (numServer - 1)) { 
        serverLoaded = true;
        System.out.println("Server is loaded and READY!");
      }
      return "";
    } else if (tokens[0].equals("requestLock") && tokens.length == 1) {
      return "";
    } else if (tokens[0].equals("ack") && tokens.length == 1) {
      return "";
    } else if (tokens[0].equals("seatReserved") && tokens.length == 3) {
      return "";
    } else if (tokens[0].equals("seatUnreserved") && tokens.length == 2) {
      return "";
    } else {
      return null;
    }
  }

  private String getHostFromHostString(String hostString) { 
    return hostString.split(":")[0];
  }

  private int getPortFromHostString(String hostString) { 
    return Integer.parseInt(hostString.split(":")[1]);
  }
  
  public void start(int myID, int numServer, int numSeat, Map<Integer, String> serverIdToReplicaHostStringMap) {

    try {

      this.numServer = numServer;

      int numReplicaConnectionsRemaining = this.numServer - 1;

      String myHostString = serverIdToReplicaHostStringMap.get(myID);

      serverIdToReplicaHostStringMap.remove(myID);

      // Start the TCP server.
      TcpServerThread tcpServerThread = new TcpServerThread(getHostFromHostString(myHostString), getPortFromHostString(myHostString), this);
      tcpServerThread.start();

      // Start the TCP clients to the replica servers.
      for (Integer serverId : serverIdToReplicaHostStringMap.keySet()) {
        String replicaHostString = serverIdToReplicaHostStringMap.get(serverId);
        System.out.println("Waiting on connection acceptance from " + replicaHostString + ". [" + numReplicaConnectionsRemaining + " replica connections remaining before accepting client commands].");
        Socket socket = null;
        while (true) {
          try {
            String hostname = getHostFromHostString(replicaHostString);
            int port = getPortFromHostString(replicaHostString);
            socket = new Socket(hostname, port);
            break;
          } catch (Exception ex) {
            // Do nothing.
          }
        }

        TcpReplicaClient replicaClient = new TcpReplicaClient(socket, this);
        serverIdToReplicaClientMap.put(serverId, replicaClient);

        replicaClient.requestFullSync();


        numReplicaConnectionsRemaining--;
      }

 
      tcpServerThread.join();


      for (Integer serverId : serverIdToReplicaClientMap.keySet()) {
        TcpReplicaClient client = serverIdToReplicaClientMap.get(serverId);
        client.close();
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void main (String[] args) {

    Scanner sc = new Scanner(System.in);
    int myID = sc.nextInt();
    int numServer = sc.nextInt();
    int numSeat = sc.nextInt();

    Map<Integer, String> serverIdToReplicaHostStringMap = new TreeMap<>();
    
    for (int i = 0; i < numServer; i++) {
      serverIdToReplicaHostStringMap.put(i + 1, sc.next());
    }

    System.out.println("");
    System.out.println("###############################################");
    System.out.println("SERVER " + myID + " is starting!");
    System.out.println("Number of replica servers: " + numServer);
    System.out.println("Number of seats: " + numSeat);
    for (Integer serverId : serverIdToReplicaHostStringMap.keySet()) {
      String replicaHostString = serverIdToReplicaHostStringMap.get(serverId);
      System.out.println("Replica Server: " + replicaHostString);
    }
    System.out.println("###############################################");

    Server server = new Server();
    server.start(myID, numServer, numSeat, serverIdToReplicaHostStringMap);

  }
}
