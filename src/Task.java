

import java.io.*;
import java.net.*;

import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

public class Task implements Runnable {
	
	private int id;
	private Socket socket = null;
	
	public Task(int id, Socket socket) {
		this.id = id;
		this.socket = socket;
	}

	public void run() {
		System.out.println("Task started, id: " + id);
		
        try (
    		InputStream input = socket.getInputStream();
			BufferedReader buffer = new BufferedReader(new InputStreamReader(input,"UTF-8"));
        ) {
        	SAXParser parser = new SAXParser();
        	StringBuilder builder = null;
        	for (String line; (line = buffer.readLine()) != null;) {
        	    if (line.startsWith("<?xml")) {
        	        if (builder != null) {
        	        	parser.parse(builder.toString());
        	        }
        	        builder = new StringBuilder();
        	    }
        	    builder.append(line);
        	}
        	
        	
        	//parser.parseXML(input);
        	
            /*String inputLine;
 
            while ((inputLine = buffer.readLine()) != null) {
            	System.out.println(inputLine);
            }*/        	
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
		
		System.out.println("Task finnished, id: " + id);
	}
	
}
