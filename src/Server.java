import java.net.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import java.io.*;


public class Server {
	
	// Dit is de BlockingQueue voor de DataBase, met een grootte van 800
	private static BlockingQueue<String> queue = new LinkedBlockingQueue<String>(3200);
	public static final String path = "/home/pi/glowmation/"; //"C:/Users/jarib/Desktop/weatherData/";
	
	
	public static void main(String[] args) {		
		try {
			// Een Thread voor DataBase, die alles uit de BlockingQueue haalt
			Thread dbThread = new Thread(new DataBase(queue));
			dbThread.start();
			
			//ExecutorService executor = Executors.newFixedThreadPool(800);			
			
			ServerSocket server_socket = new ServerSocket(7789);
			
			while(server_socket.isBound()) {				
				Socket socket = server_socket.accept();						// Accepteren van de "socket"
				Thread dataThread = new Thread(new Task(socket, queue));	// Aanmaken van de thread voor de "socket"
				dataThread.start();											// Thread starten
				//executor.execute(new Task(id, socket, queue));
			}

			
			server_socket.close();
		}
		catch (IOException e) {
			System.err.println(e);
		}
	}

}

