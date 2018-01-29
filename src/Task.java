import java.io.*;
import java.net.*;
import java.util.concurrent.BlockingQueue;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.bind.ParseConversionEvent;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.w3c.dom.Node;
import org.w3c.dom.Element;

/**
 * 2] T A S K:
 * 
 * Dit is de Thread die de data moet filteren.
 * Ook moeten hier de berekeningen worden uitgevoerd voor Exponential Growth en gevoelstemperatuur.
 * 
 * Dit zijn alle datatags die je krijgt: "STN", "DATE", "TIME", "TEMP", "DEWP", "STP", "SLP", "VISIB", "WDSP", "PRCP", "SNDP", "FRSHTT", "CLDC", "WNDDIR"
 */

public class Task implements Runnable {
	
	private int id;							// Id van de Thread
	private Socket socket = null;			// De socket die we meekrijgen van Server
	private BlockingQueue<String> queue;	// Een pointer naar de BlockingQueue van Server
	
	
	
	// Constructor van task, om de thread op te zetten
	public Task(int id, Socket socket, BlockingQueue<String> queue) {
		this.id = id;
		this.socket = socket;
		this.queue = queue;
	}

	
	
	// Dit is wat de Thread moet uitvoeren
	public void run() {
		System.out.println("Task started, id: " + id);
		
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
        	        	String xml = builder.toString();	// Hier stoppen we het document in een variabele
        	        	String parsedXML = parse(xml);		// We gebruiken onze methode(functie) "parse()" om dit document te parsen
        	        	toDatabaseBuffer( parsedXML );		// Daarna gebruiken we onze methode(functie) "toDatabaseBuffer()" om de chunk aan data in de BlockingQueue te zetten
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
		
		System.out.println("Task finnished, id: " + id);
	}
	
	
	
	// Deze methode(functie) gebruiken we om door de data heen te lopen
	// Ik gebruik het woord "chunk" voor het blok aan data per document (ongeveer 10 weerstations)
	private String parse(String xml) {
		String chunkStr = "";		// Hier stoppen we alle STN nummers achterelkaar in voor de naam van het bestand, misschien moet dit anders? (i.v.m. grote bestandsnamen)
		String datetimeStr = "";	// Aangezien de DATE/TIME in een document voor elk weerstation hetzelfde is, scheiden we dit van de data. (Zetten we 1keer neer met daarachter de data van meerdere weerstations)
		String dataStr = "";		// Hier komt de data van die meerdere weerstations in
		
		/**
		 * !!! HINT : Het resultaat wordt in een bestand dan bijvoorbeeld: !!!
		 * Naam van het bestand: "532356958374959385948337432356958206959385948395959385948395.txt" << Dit zijn dus alle STN nummers van dat blok, maar dan achter elkaar geplakt. (Eigenlijk telang, misschien alleen de eerste gebruiken ofzow?)
		 * Het bestand van binnen: "2018-01-29 12:26:29 90583882340593402935" << DATE TIME DATA(van 1e) DATA(van 2e) DATA(van 3e) etc. t/m dus ongeveer 10
		 */
		
	    try {
	    	// Dit is allemaal om de DOM Parser op te zetten
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(new InputSource(new ByteArrayInputStream(xml.getBytes("utf-8"))));
			doc.getDocumentElement().normalize();
			NodeList nList = doc.getElementsByTagName("MEASUREMENT");			
	        
			// Met deze for loop gaan we door alle weerstations binnen het xml document (dat blok van ongeveer 10 STNs)
			for (int n = 0; n < nList.getLength(); n++) {
				Node nNode = nList.item(n);
				if (nNode.getNodeType() == Node.ELEMENT_NODE) {
			        Element eElement = (Element) nNode;
			        
			        // Hier voegen we de STN nummers aan de chunStr, om zo die lange reeks aan STN nummers te krijgen voor de bestandsnaam
			        chunkStr += eElement.getElementsByTagName("STN").item(0).getTextContent();
			        
			        // Hier wordt per weerstation de datum en tijd gezet in datetimeStr. Deze variabele wordt steeds overschreven, maar dat maakt niet uit aangezien de DATE en TIME voor alle STNs op dat moment in het document hetzelfde zijn.
			        datetimeStr = eElement.getElementsByTagName("DATE").item(0).getTextContent() + " " + eElement.getElementsByTagName("TIME").item(0).getTextContent();
			        
			        // Dit is een array aan tags, zonder de STN, DATE en TIME
			        String[] items = {"TEMP", "DEWP", "STP", "SLP", "VISIB", "WDSP", "PRCP", "SNDP", "FRSHTT", "CLDC", "WNDDIR"};
			        
			        // In deze for loop lopen we door de array aan tags heen
			        for(int i = 0; i < items.length; i++){
			        	// Hier stoppen we de waarde van de tag items[i], de tag waar we op dat moment zijn in de loop, in de variabele "value"
			        	String value = eElement.getElementsByTagName(items[i]).item(0).getTextContent();
			        	
			        	/**
			        	 * !!!!!!!!!!!! DIT IS HET STUK WAAR NAAR GEKEKEN MOET WORDEN !!!!!!!!!!!
			        	 * Hier moeten de text waardes dus omgezet worden naar getallen en foutieve data eruit worden gehaald
			        	 * Om die Exponential Growth te berekenen, moet de data op één of andere manier ergens worden opgeslagen (om de week ofzow)
			        	 */
			        	
			        	// Als er ergens niks inzit, dan stoppen we met parsen want dan ontbreekt er data
			        	if( value.trim().equals("") )
			        		return null;
			        	
			        	// Hier probeerde ik de value om te zetten van String naar float, wat helaas een error geeft
			        	// float getal = Float.parseFloat( eElement.getElementsByTagName(items[i]).item(0).getTextContent() );
			        	
			        	// Via String.format("%03d", value); wou ik proberen om het getal even groot qua characters te maken. 1 is dan b.v. 0001
			        	// Maar ook dit werkte niet doordat de getallen verschillend zijn en ook b.v. negatief kunnen zijn. Getallen als -3.24
			        	
			        	// Hier voegen we de value toe aan de reeks data
			        	dataStr += value;
			        }
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	    // Uiteindelijk voeg ik hier alles aan elkaar toe. De , heb ik gebruikt om de bestandsnaam te scheiden van de rest
	    // Deze kan je nu dus niet gebruiken als scheidings teken. Aangezien in database naar de komma wordt gekeken. Dit is natuurlijk gewoon aan te passen.
	    // De : is meer om beter te kunnen zien waar de DATE/TIME bevindt voor de reeks aan data. Deze moet uiteraard uiteindelijk weg, om zo weinig mogelijk recourses te gebruiken.
	    return (chunkStr + "," + datetimeStr + " : " + dataStr);
	}
	
	
	
	// Deze functie zet de chunk aan data in de BlockingQueue
	private synchronized void toDatabaseBuffer(String chunk) {
		// Niks doen als de String null of leeg is
		if( chunk == null || chunk.trim().equals("") )
			return;
		
		// Anders stop het in de queue
    	try {
			queue.put( chunk );
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
}
