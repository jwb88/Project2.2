import java.net.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.io.*;

public class Server {
	
	private static BlockingQueue<String> queue = new LinkedBlockingQueue<String>(800);
	
	public static void main(String[] args) {
		try {
			Thread dbThread = new Thread(new DataBase(queue));
			dbThread.start();
			
			ServerSocket server_socket = new ServerSocket(7789);
			int id = 0;
			
			while(server_socket.isBound()) {
				Socket socket = server_socket.accept();
				Thread dataThread = new Thread(new Task(id, socket, queue));
				dataThread.start();
				id++;
			}
			
			server_socket.close();
		}
		catch (IOException e) {
			System.err.println(e);
		}
	}

}

