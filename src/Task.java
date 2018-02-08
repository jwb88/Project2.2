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
	
	private Socket socket = null;				// De socket die we meekrijgen van Server
	private BlockingQueue<String> queue;		// Een pointer naar de BlockingQueue van Server
	private static final int DATASIZE = 111;	// 10(STNs) * 11(Tags)
	
	
	// For the new Data
	private String chunkStr;	
	private String datetimeStr;
	private float[] newDataFloats;
	private boolean[] foutievedata;
	private boolean[] istemperatuur;
	
	// For the old Data
	private String oldDataStr = "";		// Dit is alle oude data die terug geplaatst moet worden
	private float[] averages = new float[DATASIZE];
	private int bufferSize = 0;
	
	
	public Task(Socket socket, BlockingQueue<String> queue) {
		this.socket = socket;
		this.queue = queue;
	}
	
	
	public void run() {		
        try (
    		InputStream input = socket.getInputStream();
			BufferedReader buffer = new BufferedReader(new InputStreamReader(input,"UTF-8"));
        ) {
        	StringBuilder builder = null;
        	for (String line; (line = buffer.readLine()) != null;) {
        	    if (line.startsWith("<?xml")) {
        	        if (builder != null) {
        	        	// Resetting the variables
        	        	chunkStr = "";
        	        	newDataFloats = new float[DATASIZE];
        	        	foutievedata = new boolean[DATASIZE];
        	        	istemperatuur = new boolean[DATASIZE];
        	        	oldDataStr = "";
        	        	averages = new float[DATASIZE];
        	        	bufferSize = 0;
        	        	
        	        	String xml = builder.toString();
        	        	
        	        	try {
        	        		parse(xml);
        	        	} catch (Exception e) {
							e.printStackTrace();
						}
        	        	
    	        		readingHistory();
    	        		
        	        	String dataStr = calculation();
        	        	if( dataStr != null ) {
        	        		String totalDataStr = datetimeStr + "=" + dataStr + "\n" + oldDataStr;	        	        	
	        	        	toDatabaseBuffer( chunkStr + ";" + totalDataStr );
        	        	} else {
        	        		//System.out.println("Error: chunkStr = " + chunkStr + "\ndataStr = " + dataStr + "\ndatetimeStr = " + datetimeStr + "\noldDataStr = " + oldDataStr );
        	        	}
        	        }
        	        builder = new StringBuilder();
        	    }
        	    builder.append(line);
        	}
        	
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
	}	
	
	
	
	private void parse(String xml) throws Exception {
    	byte counter = 0; // 11(tags) * 10(STNs) = <128
    	
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		Document doc = dBuilder.parse(new InputSource(new ByteArrayInputStream(xml.getBytes("utf-8"))));
		doc.getDocumentElement().normalize();
		NodeList nList = doc.getElementsByTagName("MEASUREMENT");
		
		for (int n = 0; n < nList.getLength(); n++) {
			Node nNode = nList.item(n);
			if (nNode.getNodeType() == Node.ELEMENT_NODE) {
		        Element eElement = (Element) nNode;
		        
		        String newChunk = eElement.getElementsByTagName("STN").item(0).getTextContent();
		        chunkStr += fixedLengthString(newChunk, 6, '0');

		        datetimeStr = eElement.getElementsByTagName("DATE").item(0).getTextContent() + "_" + eElement.getElementsByTagName("TIME").item(0).getTextContent();
		        
		        String[] items = {"TEMP", "DEWP", "STP", "SLP", "VISIB", "WDSP", "PRCP", "SNDP", "FRSHTT", "CLDC", "WNDDIR"};
		        
		        for(int i = 0; i < items.length; i++){
		        	String value = eElement.getElementsByTagName(items[i]).item(0).getTextContent();
		        	
		        	if( i == 0 ) {
		        		istemperatuur[counter] = true;
		        	}
		        	
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
	}
	
	
	
	private void readingHistory() {
		File f = new File(Server.path + chunkStr + ".txt");
		String text = null;
		int readingCounter = 0;
		
		float[] prev_numbers = new float[DATASIZE];
		
		while( readingCounter < 3 ) {
			if( !f.exists() || f.isDirectory() )
				break;
			
			if( f.canWrite() ) {
				try (BufferedReader reader = new BufferedReader(new FileReader(Server.path + chunkStr + ".txt"))) {
					boolean prev_available = false;
					while ((text = reader.readLine()) != null) {
						// Buffer +1, dus 29+1=30
						if( bufferSize < 29 ) {
							oldDataStr += text + "\n";							
							String[] oldLine = text.split("=");
							
							try {								
								if( oldLine[1].trim().length() > 0 ) {
									//String oldDateTime = oldLine[0];
									
									//System.out.println(oldLine[1].split(",").length + " lengte in " + chunkStr + ".txt");
									/*if( oldLine[1].split(",").length > dataSize ) {
										System.out.println("Error: lengte = " + oldLine[1].split(",").length + " in: " + chunkStr);
									}*/
									
									String[] oldData = new String[DATASIZE];
									try {
										oldData = oldLine[1].split(",");
									} catch (Exception e) {
										System.out.println("error: " + text + " In " + chunkStr );
									}
									float[] oldDataFloats = new float[DATASIZE];
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
								bufferSize++;
							} catch (Exception e) {
								//System.out.println(chunkStr + ": " + text);
								//e.printStackTrace();
							}
						}
				    }
					break;
				} catch (IOException e) {
					e.printStackTrace();
				}					
			}
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			readingCounter++;
		}
	}
	
	
	
	private String calculation() {
		String dataStr = "";
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
						dataStr += String.format(Locale.US, "%.2f", extrapolatie) + splitChar;
					} else {
						dataStr += String.format(Locale.US, "%.2f", newDataFloats[i]) + splitChar;
					}
				} else {
					dataStr += String.format(Locale.US, "%.2f", newDataFloats[i]) + splitChar;
				}
			} else {
				if( foutievedata[i] ) {
					//System.err.println("Skip save, because number: " + newDataFloats[i]);
					return null;
				} else {
					dataStr += String.format(Locale.US, "%.2f", newDataFloats[i]) + splitChar; //Float.toString(newDataFloats[i]) + ",";
				}
			}
		}
		//System.out.println(dataStr);
		return dataStr;
	}
	
	
	
	private String fixedLengthString(String string, int len, char fill) {
		return new String(new char[len - string.length()]).replace('\0', fill) + string;
	}
	
	
	
	
	private synchronized void toDatabaseBuffer(String chunk) {		
		if( chunk == null || chunk.trim().equals("") )
			return;
		
    	try {
			queue.put( chunk );
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
}
