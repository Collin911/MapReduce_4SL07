import java.io.*;
import java.net.*;
import java.util.function.Consumer;

public class ServerHandler {
    private ServerSocket serverSocket;
    private boolean running = false;

    public void startServer(int port, Consumer<Message> onMessage) throws IOException {
        serverSocket = new ServerSocket(port);
        running = true;
        new Thread(() -> {
            try {
                while (running) {
                    Socket client = serverSocket.accept();
                    ObjectInputStream ois = new ObjectInputStream(client.getInputStream());
                    Message msg = (Message) ois.readObject();
                    onMessage.accept(msg);
                    ois.close();
                    client.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }

    public void stopServer() throws IOException {
        running = false;
        serverSocket.close();
    }

    public void sendMessage(String ip, int port, Message msg) throws IOException {
        Socket socket = new Socket(ip, port);
        ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
        oos.writeObject(msg);
        oos.close();
        socket.close();
    }
}