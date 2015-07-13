package twitter_filtering_stefano;

import java.awt.Color;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Spliterators.AbstractSpliterator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

/**
 * Implements an iterator capable of feeding tweets received 'live' to a java 8 stream.
 * The stream is terminated either if the {@link #stop()} method is called externally or
 * when the duration specified in capturing.conf elapsed since the instant the stream was created.
 * 
 * @author stefano
 *
 */
public class OnlineIterator extends StoppableSpliterator<Tweet> implements StatusListener {

	private Config config;
	private LinkedBlockingQueue<Tweet> buffer;
	private MessageConsole console;
	
	private TwitterStream twitterStream;
	private FilterQuery filter; // contains twitter query parameters
	private Instant stopInstant; // when automatically stop receiving tweets
	
	private DbWriter dbWriter; // parallel database accessing thread
	
	private boolean started = false; // used to start capturing online tweets when the first request is received by the streaming api 
	
	protected OnlineIterator(Config config, Instant startInstant, MessageConsole console, MessageConsole dbConsole) throws ClassNotFoundException, SQLException {
		super(0, AbstractSpliterator.ORDERED | AbstractSpliterator.IMMUTABLE);
		
		this.config = config;
		this.console = console;
		buffer = new LinkedBlockingQueue<>();
		
		stopInstant = startInstant.plusMillis(config.getDuration());
		
		twitterStream = new TwitterStreamFactory().getInstance();
		twitterStream.addListener(this);
		
		String[] keywords = config.getKeywords().toArray(new String[0]);
		String[] languages = { config.getLng() };
		filter = new FilterQuery(0, new long[0], keywords, new double[0][0], languages);
		
		// store tweets in the 'dbTable' table of the database specified in the configuration object.
		dbWriter = new DbWriter(config, "INSERT INTO `" + config.getDbTable() +"` VALUES (?,?,?)", dbConsole);
		dbWriter.start();
	}

	@Override
	public boolean tryAdvance(Consumer<? super Tweet> consumer) {
		if(!started) {
			twitterStream.filter(filter);
			started = true;
		}
		
		try {
			Tweet tweet = buffer.take();

			if(stop && tweet.getId() < 0) {
				dbWriter.finish();
				return false;
			}
			
			// store the tweet in DB 'filtering' table
			// enqueue a copy of the tweet to avoid that the (asynchronous) query 
			// will insert modified (i.e. filtered) data in the database
			dbWriter.enqueue(new Tweet(tweet));
			
			console.write("analyzing tweet id: " + tweet.getId() + " timestamp: " + tweet.getTimestampAsString());
			
			consumer.accept(tweet);
		} catch (InterruptedException e) {
			console.write(e.getMessage(), Color.red);
		}
		return true;
	}
	
	/**
	 * Stop the reception of tweets from online stream 
	 */
	@Override
	public void stop() {
		super.stop();
		buffer.add(new Tweet(-1L, "", Instant.now())); // 'poison' message, just to wake up the queue 
		console.write("stopping online capturing...", Color.green);
		twitterStream.clearListeners();
		twitterStream.cleanUp();
		twitterStream.shutdown();
	}
	
	@Override
	public void onStatus(Status status) {
		// apply configurations
    	if(status.getInReplyToStatusId()==-1 && !config.isKeepReply()) return;
    	if(status.isRetweet() && !config.isKeepRetweet()) return;
    	
    	// build the tweet
    	long id = status.getId();
    	String text = status.getText().toLowerCase();
		Instant timestamp = status.getCreatedAt().toInstant();
		Tweet tweet = new Tweet(id, text, timestamp);
		
    	if(timestamp.isAfter(stopInstant)) {
    		stop();
    	}
    	
    	// store thw tweet in the iterator queue 
		buffer.add(tweet);
	}

	@Override
	public void onException(Exception e) {
		console.write(e.getMessage(), Color.red);
	}

	@Override
	public void onDeletionNotice(StatusDeletionNotice arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onScrubGeo(long arg0, long arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onStallWarning(StallWarning arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onTrackLimitationNotice(int arg0) {
		// TODO Auto-generated method stub
		
	}
}
