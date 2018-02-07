import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.w3c.dom.Node;
import org.w3c.dom.Element;


public class Task implements Runnable {
	
	private int id;							// Id van de Thread
	private Socket socket = null;			// De socket die we meekrijgen van Server
	private BlockingQueue<String> queue;	// Een pointer naar de BlockingQueue van Server
	private static int dataSize = 111;		// 10(STNs) * 11(Tags)
	
	
	
	public Task(int id, Socket socket, BlockingQueue<String> queue) {
		this.id = id;
		this.socket = socket;
		this.queue = queue;
	}
	
	
	public void run() {
		//System.out.println("Task started, id: " + id);
		
        try (
        	// Proberen ophalen InputStream van de socket. 
        	// Deze zetten we in een BufferedReader(een buffer die mee schaalt met hoeveel er via de socket binnenkomt)
    		InputStream input = socket.getInputStream();
			BufferedReader buffer = new BufferedReader(new InputStreamReader(input,"UTF-8"));
        ) {
        	// Als dat gelukt is, gebruiken we een StringBuilder om de binnengekomen data te scheiden op XML document
        	StringBuilder builder = null;
        	for (String line; (line = buffer.readLine()) != null;) {
        	    if (line.startsWith("<?xml")) {
        	        if (builder != null) {
        	        	String xml = builder.toString();
        	        	String parsedXML = parse(xml);
        	        	toDatabaseBuffer( parsedXML );
        	        }
        	        builder = new StringBuilder();
        	    }
        	    builder.append(line);
        	}
        	
        	// En als laatste sluiten we de "socket"
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
		
		//System.out.println("Task finnished, id: " + id);
	}	
	
	
	
	private String parse(String xml) {
		String chunkStr = "";
		String totalDataStr = "";
		
	    try {
	    	float[] newDataFloats = new float[dataSize];
	    	boolean[] foutievedata = new boolean[dataSize];
	    	boolean[] istemperatuur = new boolean[dataSize];
	    	byte counter = 0; // 11(tags) * 10(STNs) = <128
	    	
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(new InputSource(new ByteArrayInputStream(xml.getBytes("utf-8"))));
			doc.getDocumentElement().normalize();
			NodeList nList = doc.getElementsByTagName("MEASUREMENT");
	        
			
			String datetimeStr = "";
			
			for (int n = 0; n < nList.getLength(); n++) {
				Node nNode = nList.item(n);
				if (nNode.getNodeType() == Node.ELEMENT_NODE) {
			        Element eElement = (Element) nNode;
			        
			        String newChunk = eElement.getElementsByTagName("STN").item(0).getTextContent();
			        chunkStr += fixedLengthString(newChunk, 6, '0');
			        //chunkStr += eElement.getElementsByTagName("STN").item(0).getTextContent();

			        datetimeStr = eElement.getElementsByTagName("DATE").item(0).getTextContent() + "_" + eElement.getElementsByTagName("TIME").item(0).getTextContent();
			        
			        String[] items = {"TEMP", "DEWP", "STP", "SLP", "VISIB", "WDSP", "PRCP", "SNDP", "FRSHTT", "CLDC", "WNDDIR"};
			        
			        for(int i = 0; i < items.length; i++){
			        	String value = eElement.getElementsByTagName(items[i]).item(0).getTextContent();
			        	
			        	if( i == 0 ) {
			        		istemperatuur[counter] = true;
			        	}
			        	
			        	// Als er ergens niks inzit, dan stoppen we er wat in?
			        	if( value.trim().equals("") ) {
			        		newDataFloats[counter] = 0;
			        		foutievedata[counter] = true;
			        	} else {			        		
			        		newDataFloats[counter] = Float.parseFloat( eElement.getElementsByTagName(items[i]).item(0).getTextContent() );
			        	}
			        	counter++;
			        }
				}
			}
			
			
			
			String oldDataStr = "";		// Dit is alle oude data die terug geplaatst moet worden
			float[] prev_numbers = new float[dataSize];
			float[] averages = new float[dataSize];
			File f = new File(Server.path + chunkStr + ".txt");
			//BufferedInputStream check = new BufferedInputStream(new FileInputStream(Server.path + chunkStr + ".txt"));
			String text = null;
			int bufferSize = 0;
			if(f.exists() && !f.isDirectory() && f.canWrite()) {
				try (BufferedReader reader = new BufferedReader(new FileReader(Server.path + chunkStr + ".txt"))) {
					boolean prev_available = false;
					while ((text = reader.readLine()) != null) {
						// Buffer +1, dus 29+1=30
						if( bufferSize < 29 ) {
							oldDataStr += text + "\n";							
							String[] oldLine = text.split("=");
							
							if( oldLine[1].trim().length() > 0 ) {
								//String oldDateTime = oldLine[0];
								
								//System.out.println(oldLine[1].split(",").length + " lengte in " + chunkStr + ".txt");
								
								
								/*if( oldLine[1].split(",").length > dataSize ) {
									System.out.println("Error: lengte = " + oldLine[1].split(",").length + " in: " + chunkStr);
								}*/
								
								String[] oldData = new String[dataSize];
								try {
									oldData = oldLine[1].split(",");
								} catch (Exception e) {
									System.out.println("error: " + text + " In " + chunkStr );
								}
								float[] oldDataFloats = new float[dataSize];
								for(int i = 0; i < oldData.length; i++){ // was eerst tot oldData.length
									//System.out.println(oldData[i]);
									float number = Float.parseFloat(oldData[i]);
									oldDataFloats[i] = number;
									if( prev_available ) {
										// Hier berekening en in avarages stoppen
										averages[i] += prev_numbers[i] - number;
									}
									prev_numbers[i] = number;
								}
								prev_available = true;
							}
						}
				        bufferSize++;				        
				    }
				} catch (IOException e) {
					e.printStackTrace();
				}
			} else {
				//System.err.println("Buffer not available :C yet..");
			}
			
			
			
			String dataStr = "";		// Dit is alle nieuwe data die we toevoegen
			for(int i = 0; i < newDataFloats.length; i++) {
				String splitChar = ",";
				if( i == newDataFloats.length-1 ) {
					splitChar = "";
				}
				if( bufferSize > 2 ) {
					float extrapolatie = (averages[i] / (bufferSize-1)) + newDataFloats[i];
					
					if( foutievedata[i] ) {						
						dataStr += String.format(Locale.US, "%.2f", extrapolatie) + splitChar;
					} else if( istemperatuur[i] ) {
						float verschilProcenten = ((extrapolatie - newDataFloats[i]) / newDataFloats[i]) * 100;
						//System.out.println("data: " + newDataFloats[i] + ", extrapolatie: " + extrapolatie + ", procentverschil: " + verschilProcenten);
						
						if( verschilProcenten <= 20 || verschilProcenten >= 20 ) {
							//newDataFloats[i] = extrapolatie; inplaats van er terug in te stoppen, doen we nu:
							dataStr += String.format(Locale.US, "%.2f", extrapolatie) + splitChar; //Float.toString(extrapolatie) + ",";
						} else {
							dataStr += String.format(Locale.US, "%.2f", newDataFloats[i]) + splitChar;
						}
					} else {
						dataStr += String.format(Locale.US, "%.2f", newDataFloats[i]) + splitChar;
					}
				} else {
					if( foutievedata[i] ) {
						//System.err.println("Skip save");
						return null;
					} else {
						dataStr += String.format(Locale.US, "%.2f", newDataFloats[i]) + splitChar; //Float.toString(newDataFloats[i]) + ",";
					}
				}
			}
			
			//System.out.println("DATASTRING = " + datetimeStr + "=" + dataStr);
			totalDataStr = datetimeStr + "=" + dataStr + "\n" + oldDataStr;
		} catch (Exception e) {
			e.printStackTrace();
		}
	    return ( chunkStr + ";" + totalDataStr );
	}
	
	
	
	private String fixedLengthString(String string, int len, char fill) {
		return new String(new char[len - string.length()]).replace('\0', fill) + string;
	}
	
	
	
	// Deze functie zet de chunk aan data in de BlockingQueue
	private synchronized void toDatabaseBuffer(String chunk) {		
		if( chunk == null || chunk.trim().equals("") )	// Niks doen als de String null of leeg is
			return;
		
		// Anders stop het in de queue
    	try {
			queue.put( chunk );
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
}
