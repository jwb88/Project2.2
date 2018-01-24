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
			String nextBox = queue.poll();
			if( nextBox != null ) {
				System.out.println( "Database: insert(" + nextBox + ")" );
			}
			try {
				Thread.sleep(4);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
}
