import java.net.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.io.*;

/**
 * 1] S E R V E R:
 * 
 * Deze luistert naar de client(generator) en maakt voor elk een nieuwe "dataThread"(Task) aan.
 * Ook wordt hier de BlockingQueue en een "dbThread" voor de DataBase aangemaakt.
 * 
 * TODO : Naar mijn idee hoeft er niks in dit bestand aangepast te worden.
 */

public class Server {
	
	// Dit is de BlockingQueue voor de DataBase, met een grootte van 800
	private static BlockingQueue<String> queue = new LinkedBlockingQueue<String>(800);
	
	
	
	public static void main(String[] args) {
		try {
			// Een Thread voor DataBase, die alles uit de BlockingQueue haalt
			Thread dbThread = new Thread(new DataBase(queue));
			dbThread.start();
			
			// Hier openen we de poort voor de generator + Een id voor de threads
			ServerSocket server_socket = new ServerSocket(7789);
			int id = 0;
			
			// Oneindige loop, wanneer de ServerSocket een verbinding heeft
			while(server_socket.isBound()) {
				Socket socket = server_socket.accept();							// Accepteren van de "socket"
				Thread dataThread = new Thread(new Task(id, socket, queue));	// Aanmaken van de thread voor de "socket"
				dataThread.start();												// Thread starten
				id++;
			}
			
			// Als het niet gelukt is, dan sluiten we de ServerSocket
			server_socket.close();
		}
		catch (IOException e) {
			System.err.println(e);
		}
	}

}

