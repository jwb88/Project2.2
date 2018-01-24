import java.io.*;
import java.net.*;
import java.util.concurrent.BlockingQueue;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.w3c.dom.Node;
import org.w3c.dom.Element;

public class Task implements Runnable {
	
	private int id;
	private Socket socket = null;
	private BlockingQueue<String> queue;
	
	public Task(int id, Socket socket, BlockingQueue<String> queue) {
		this.id = id;
		this.socket = socket;
		this.queue = queue;
	}

	public void run() {
		System.out.println("Task started, id: " + id);
		
        try (
    		InputStream input = socket.getInputStream();
			BufferedReader buffer = new BufferedReader(new InputStreamReader(input,"UTF-8"));
        ) {
        	StringBuilder builder = null;
        	for (String line; (line = buffer.readLine()) != null;) {
        	    if (line.startsWith("<?xml")) {
        	        if (builder != null) {
        	        	String xml = builder.toString(); //.replaceAll("\\s+","");
        	        	// System.out.println( xml );
        	        	toDatabaseBuffer( parse(xml) );
        	        }
        	        builder = new StringBuilder();
        	    }
        	    builder.append(line);
        	}
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
		
		System.out.println("Task finnished, id: " + id);
	}
	
	private String parse(String xml) {
		String parsedString = "";
	    try {
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(new InputSource(new ByteArrayInputStream(xml.getBytes("utf-8"))));
			doc.getDocumentElement().normalize();
			NodeList nList = doc.getElementsByTagName("MEASUREMENT");			
	          
			for (int n = 0; n < nList.getLength(); n++) {
				Node nNode = nList.item(n);
				if (nNode.getNodeType() == Node.ELEMENT_NODE) {
			        Element eElement = (Element) nNode;
			        
			        // "stn", "date", "time", "temp", "dewp", "stp", "slp", "visib", "wdsp", "prcp", "sndp", "frshtt", "cldc", "wnddir"
			        
			        // TODO : All data + check for false data (regex)
			        // TODO : Store data on PI to calculate
			        
			        String[] items = {"STN", "DATE", "TIME", "TEMP", "DEWP", "STP", "SLP", "VISIB", "WDSP", "PRCP", "SNDP", "FRSHTT", "CLDC", "WNDDIR"};
			        
			        for(int i = 0; i < items.length; i++){
			        	parsedString += eElement.getElementsByTagName(items[i]).item(0).getTextContent();
			        }
			        //parsedString += (stn + date + time + temp);
				}
			}
	          
		} catch (Exception e) {
			e.printStackTrace();
		}
	    return parsedString;
	}
	
	private synchronized void toDatabaseBuffer(String data) {
		if( data == null )
			return;
		
    	try {
			queue.put( data );
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
}
