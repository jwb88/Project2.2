import java.io.*;
import java.net.*;

public class DataThread implements Runnable {
	
	private int id;
	private Socket socket = null;
	private byte[] buffer;

	public DataThread(int id, Socket socket) {
		this.id = id;
		this.socket = socket;
		buffer = new byte[4096]; // or 4096, or more
	}
	
	@Override
	public void run() {
		System.out.println("DataThread id: " + id + " started!");

		try {
			InputStream in = socket.getInputStream();
			
			//in.read(buffer);
			System.out.println( new String(buffer, 0, buffer.length).replaceAll("\n", "") );
			
			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		System.out.println("DataThread id: " + id + " done!");
	}
	
}
