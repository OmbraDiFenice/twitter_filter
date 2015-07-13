package twitter_filtering_stefano;

import java.awt.Color;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Handle the interaction with the database. This class implements a parallel thread that receive some tweets
 * and execute the specified prepared query on them.
 * 
 * @author stefano
 *
 */
public class DbWriter extends Thread {

	private MessageConsole console;
	private LinkedBlockingQueue<Tweet> queue;
	
	private MySQLBridge dbConn;
	private boolean stop = false;
	
	private int values = 0;
	private final int threshold = 200;
	private PreparedStatement query;
	
	public DbWriter(Config config, String preparedQuery, MessageConsole console) throws ClassNotFoundException, SQLException {
		this.console = console;
		
		// connect to database
		dbConn = new MySQLBridge(config.getDbAddress(), config.getDbUser(), config.getDbPassword(), config.getDbSchema());
		
		// prepare query
		query = dbConn.getConnection().prepareStatement(preparedQuery);
		
		queue = new LinkedBlockingQueue<>();
	}
	
	/**
	 * Store the received tweet in the database. This method actually performs the query only when either a threshold number
	 * of tweets have been received or when the tweet input queue is empty, i.e. further waiting would waste time.
	 * 
	 * @param tweet the tweet to be stored in the database
	 */
	private void storeInDB(Tweet tweet) {
		try {
			// ignore 'poison' element, see https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/BlockingQueue.html
			if(tweet.getId() > 0) {
				query.setLong(1, tweet.getId());
				query.setString(2, tweet.getText());
				//query.setString(3, tweet.getTimestampAsString());
				query.setDate(3, new java.sql.Date(Date.from(tweet.getTimestamp()).getTime()));
				query.addBatch();
				
				values++;
			}
			
			// this check must be done outside the above if, otherwise the last tweets won't be written in the DB
			if(values >= threshold || queue.isEmpty()) {
				query.executeBatch();
				dbConn.getConnection().commit();
				
				console.write(values + " tweets stored in DB");
				query.clearBatch();
				values = 0;
			}
		} catch (SQLException e) {
			console.write(tweet.toString(), Color.red);
			console.write(e.getMessage(), Color.red);
		}
	}

	/**
	 * Add a tweet to the queue of tweets waiting to be written in the database.
	 * 
	 * @param tweet tweet to add to the database
	 */
	public void enqueue(Tweet tweet) {
		try {
			queue.put(tweet);
		} catch (InterruptedException e) {
			console.write(e.getMessage(), Color.red);
		}
	}
	
	/**
	 * Signal the end of tweet stream. This forces the flush of the enqueued tweets to be stored in the database, if any.
	 */
	public void finish() {
		stop = true;
		queue.add(new Tweet(-1L, "", Instant.now())); // insert 'poison' element to unlock the waiting queue, see https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/BlockingQueue.html
		console.write("received 'finish' signal, emptying queue...", Color.green);
	}

	@Override
	public void run() {
		while(!stop || !queue.isEmpty()) {
			try {
				storeInDB(queue.take());
			} catch (InterruptedException e) {
				console.write(e.getMessage(), Color.red);
			}
		}
		console.write("closing DB connection...", Color.green);
		dbConn.closeConnection();
		console.write("done.", Color.green);
	}
	
}
