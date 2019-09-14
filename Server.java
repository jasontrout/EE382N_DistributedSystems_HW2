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


    class Order {

      private int orderId;
      private String productName;
      private int productQuantity;

      public Order(int orderId, String productName, int productQuantity) {
        this.orderId = orderId;
        this.productName = productName;
        this.productQuantity = productQuantity;
      }

      public int getOrderId() {
        return orderId;
      }

      public String getProductName() {
        return productName;
      }

      public int getProductQuantity() {
        return productQuantity;
      }

      @Override
      public boolean equals(Object o) {
        if (!(o instanceof Order)) return false;
        Order rhsOrder = (Order)o;
        if (orderId == rhsOrder.getOrderId() && 
            productName.equals(rhsOrder.getProductName()) &&
            productQuantity == rhsOrder.getProductQuantity()) {
          return true;
        }
        return false;
      }

      @Override
      public int hashCode() {
        return Objects.hash(orderId, productName, productQuantity);
      }


    } 

    private Map<String, Integer> productMap = new TreeMap<>();
    private Map<String, Map<Integer, Order>> userNameToOrdersMap = new TreeMap<>();
    private Map<Integer, String> orderIdToUsernameMap = new TreeMap<>();
    private int orderIdCounter = 1;

    public DataInterface() { }

    // Load inventory from a file.
    public synchronized void loadInventoryFromFile(String fileName) throws FileNotFoundException {
      File file = new File(fileName);
      Scanner sc = new Scanner(file);
      while (sc.hasNextLine()) { 
        String name = sc.next(); 
    	  int quantity = sc.nextInt();
	      productMap.put(name, quantity);
      }
      sc.close();
    }

    // Purchase.
    public synchronized String purchase(String username, String productName, int productQuantity) {
      StringBuilder sb = new StringBuilder();
      if (productQuantity < 1) {
        sb.append("At least one of the product must be purchased for an order");
      } else if (!productMap.containsKey(productName)) {
        sb.append("Not Available - We do not set this product");
      } else {
        int productQuantityRemaining = productMap.get(productName);
        if (productQuantity > productQuantityRemaining) {
          sb.append("Not Available - Not enough items");
        } else {
          int orderId = orderIdCounter++;
          Order order = new Order(orderId, productName, productQuantity);
          if (!userNameToOrdersMap.containsKey(username)) {
            userNameToOrdersMap.put(username, new TreeMap<>());
          }
          userNameToOrdersMap.get(username).put(orderId, order);
          orderIdToUsernameMap.put(orderId, username);
          productMap.put(productName, productQuantityRemaining - productQuantity);
          sb.append("Your order has been placed, ").append(orderId).append(" ").append(username).append(" ").append(productName).append(" ").append(productQuantity);
        }
      }
      return sb.append("\n").toString();
    }

    // Cancel.
    public synchronized String cancel(int orderId) {
      StringBuilder sb = new StringBuilder();
      if (!orderIdToUsernameMap.containsKey(orderId)) {
        sb.append(orderId).append(" not found, no such order");
      } else {
        String username = orderIdToUsernameMap.get(orderId);
        Order order = userNameToOrdersMap.get(username).get(orderId);
        String productName = order.getProductName();
        int productQuantity = order.getProductQuantity();
        int productQuantityRemaining = productMap.get(productName);
        productMap.put(productName, productQuantityRemaining + productQuantity);
        orderIdToUsernameMap.remove(orderId);
        userNameToOrdersMap.get(username).remove(orderId);
        sb.append("Order ").append(orderId).append(" is canceled");
      }
      return sb.append("\n").toString();
    }

    // Search.
    public synchronized String search(String username) {
      StringBuilder sb = new StringBuilder();
      Map<Integer, Order> orderIdToOrderMap = userNameToOrdersMap.get(username);
      if (orderIdToOrderMap != null && orderIdToOrderMap.size() > 0) {
        for (int orderId : orderIdToOrderMap.keySet()) {
          Order order = orderIdToOrderMap.get(orderId);
          String productName = order.getProductName();
          int productQuantity = order.getProductQuantity();
          sb.append(orderId).append(", ").append(productName).append(", ").append(productQuantity).append("\n");
        }
      } else {
        sb.append("No order found for ").append(username).append("\n");
      }
      return sb.toString();
    }

    // List.
    public synchronized String list() { 
      StringBuilder sb = new StringBuilder();
      productMap.entrySet().stream().sorted(Map.Entry.<String, Integer>comparingByValue().reversed().thenComparing(Map.Entry.comparingByKey()))
        .forEach((e) -> {
        sb.append(e.getKey()).append(" ").append(e.getValue()).append("\n");
      });
      return sb.toString();
    }

  }

  private DataInterface dataInterface = new DataInterface();


  // UDP Server Thread
  class UdpServerThread extends Thread {
    private IServer server;
    private DatagramSocket socket;
    private byte[] buf = new byte[1024];
    private int port;
    private boolean running ;

    public UdpServerThread(int port, IServer server) {
      this.port = port;
      this.server = server;
    }

    public void run() {
      try {
        running = true;
        socket = new DatagramSocket(port);
        System.out.println("UDP server started. Listening on " + socket.getLocalAddress() + ":" + port);
        while (running) {
          try { 
            DatagramPacket srcPacket = new DatagramPacket(buf, buf.length);
	          socket.receive(srcPacket);

	          InetAddress srcAddress = srcPacket.getAddress();
	          int srcPort = srcPacket.getPort();


            String command = new String(srcPacket.getData(), 0, srcPacket.getLength());
            String response = server.executeCommand(command);
            
            if (response != null) {
              byte[] responseBytes = response.getBytes();
              DatagramPacket responsePacket = new DatagramPacket(responseBytes, responseBytes.length, srcAddress, srcPort);
              socket.send(responsePacket);
            }
          } catch (Exception ex) {
	          ex.printStackTrace();
          }
        }
      } catch (Exception e) {
        System.out.println("Error in UdpServerThread.");
        e.printStackTrace();
      }
    }

    @Override
    public void interrupt() {
      super.interrupt();
      running = false;
      socket.close();
    }
  }

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
    private int port;
    private boolean running;
    private int clientIdCounter = 1;
    private Map<Integer, TcpClientThread> clientIdToClientThreadMap = new TreeMap<>();
   

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
    if (tokens[0].equals("purchase") && tokens.length == 4) {
      String username = tokens[1];
      String productName = tokens[2];
      int quantity = parseInt(tokens[3]);
      return dataInterface.purchase(username, productName, quantity);
    } else if (tokens[0].equals("cancel") && tokens.length == 2) {
      int orderId = parseInt(tokens[1]);
      return dataInterface.cancel(orderId);
    } else if (tokens[0].equals("search") && tokens.length == 2) {
      String username = tokens[1];
      return dataInterface.search(username);
    } else if (tokens[0].equals("list") && tokens.length == 1) {
      return dataInterface.list();
    } else {
      return null;
    }
  }

  public void start(int tcpPort, int udpPort, String fileName) {

    try {

      // Load inventory file.
      dataInterface.loadInventoryFromFile(fileName);

      // Start the TCP server.
      TcpServerThread tcpServerThread = new TcpServerThread(tcpPort, this);
      tcpServerThread.start();

      // Start the UDP server.
      UdpServerThread udpServerThread = new UdpServerThread(udpPort, this);
      udpServerThread.start();

      // Wait for the threads to finish.
      tcpServerThread.join();
      udpServerThread.join();


    } catch (InterruptedException e) {
      System.out.println("Main thread interrupted.");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void main (String[] args) {
    int tcpPort;
    int udpPort;
    if (args.length != 3) {
      System.out.println("ERROR: Provide 3 arguments");
      System.out.println("\t(1) <tcpPort>: the port number for TCP connection");
      System.out.println("\t(2) <udpPort>: the port number for UDP connection");
      System.out.println("\t(3) <file>: the file of inventory");

      System.exit(-1);
    }

    tcpPort = Integer.parseInt(args[0]);
    udpPort = Integer.parseInt(args[1]);
    String fileName = args[2];

    Server server = new Server();
    server.start(tcpPort, udpPort, fileName);

  }
}
