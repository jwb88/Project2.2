import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;


public class DataBase implements Runnable {
	
	private BlockingQueue<String> queue;	
	
	
	public DataBase(BlockingQueue<String> queue) {
		this.queue = queue;
	}
	
	
	@Override
	public synchronized void run() {

		// TODO : Stop when closing program
		while(true) {
			String nextChunk = queue.poll();

			if( nextChunk != null ) {
				String[] chunk = nextChunk.split(";");
				
				if( chunk.length < 2 ) {
					System.err.println("error in: " + nextChunk);
				} else {
					try (BufferedWriter bw = new BufferedWriter(new FileWriter(Server.path + chunk[0] + ".txt"))) { // FileWriter 2e arg "true" to append
						bw.write(chunk[1]);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
			
			// Elke loop van deze DataBase Thread wachten we ff
			/*try {
				Thread.sleep(4);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}*/
		}
	}
	
}
