import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;

/**
 * 3] D A T A B A S E:
 * 
 * Dit is de database thread, die de data uit de queue haalt en deze opslaadt in bestanden.
 */

public class DataBase implements Runnable {
	
	// Dit is weer een pointer naar de BlockingQueue van Server
	private BlockingQueue<String> queue;
	
	
	
	// Een simpel constructortje van DataBase
	public DataBase(BlockingQueue<String> queue) {
		this.queue = queue;
	}

	
	
	@Override
	public synchronized void run() {

		// TODO : Stop when closing program
		
		// Dit is een oneindige loop in een Thread. Dus de thread stopt nooit. 
		// Ik heb hier nog geen probelemen van gehad, dus misschien doet Java dit voor ons.
		while(true) {
			// Hier halen we de volgende chunk aan data uit de queue
			String nextChunk = queue.poll();

			// Als dat niet null is
			if( nextChunk != null ) {
				String[] chunk = nextChunk.split(",");		// Dan gebruiken we split om de chunkStr van de data te scheiden
				String path = "C:/Users/jarib/Desktop/weatherData/" + chunk[0] + ".txt";	// Dit is het pad waar de files worden opgeslagen. chunk[0] is dus het stuk voor de komma, dus de chunkStr
				
				// Hier proberen we via een BufferedWriter naar het bestand te schrijven
				try (BufferedWriter bw = new BufferedWriter(new FileWriter(path, true))) {
					bw.append(chunk[1]);
				} catch (IOException e) {
					e.printStackTrace();
				}

				// Uiteindelijk printen we wat hij doet, om te zien waar het fout gaat
				System.out.println( "Opslaan chunk["+chunk[0]+"] " + chunk[1] );
			}
			
			// Elke loop van deze DataBase Thread wachten we ff
			try {
				Thread.sleep(4);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
}
