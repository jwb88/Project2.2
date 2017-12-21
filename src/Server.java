

import java.net.*;
import java.io.*;

public class Server {
	
	public static void main(String[] args) {
		try {
			ServerSocket sock = new ServerSocket(7789);
			int id = 0;
			
			// now listen for connections
			while(id==0) {
				Socket client = sock.accept();
				
				//System.out.println("Read file, creating new Thread!");
				Thread task = new Thread(new Task(id, client));
				task.start();
				id++;
			}
		}
		catch (IOException e) {
			System.err.println(e);
		}
	}

}

