package twitter_filtering_stefano;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Generate the stream of tweet. This automatically generate a stream from file or live from twitter,
 * as specified by the demo option in platform.conf
 * 
 * @author stefano
 *
 */
public class Capturing {

	private Config config;
	private MessageConsole console;
	private MessageConsole dbLog;
	private StoppableSpliterator<Tweet> iterator = null; // the iterator providing the stream
	private Instant firstWindowStart = null; // time instant of the first tweet
	
	public Capturing(Config config, MessageConsole console, MessageConsole dbLog) {
		this.config = config;
		this.console = console;
		this.dbLog = dbLog;
	}
	
	/**
	 * Set up the stream from the file specified by the file option in platform.conf
	 * 
	 * @return the stream of tweets generated from file
	 * @throws IOException if there was a problem accessing the file specified by the file option in platform.conf 
	 * @throws SQLException if there was a problem conecting to the database
	 * @throws ClassNotFoundException if the mysql connector was not found
	 */
	private Stream<Tweet> createOfflineStream() throws IOException, ClassNotFoundException, SQLException {
		iterator = new OfflineIterator(config, console, dbLog);
		firstWindowStart = ((OfflineIterator) iterator).getFirstWindowStart();
		return StreamSupport.stream(iterator, false);
	}
	
	/**
	 * Set up the live stream from twitter
	 * 
	 * @return the stream of tweets generated live from twitter 
	 * @throws SQLException if there was a problem conecting to the database
	 * @throws ClassNotFoundException if the mysql connector was not found 
	 */
	private Stream<Tweet> createOnlineStream() throws ClassNotFoundException, SQLException {
		firstWindowStart = Instant.now();
		
		iterator = new OnlineIterator(config, firstWindowStart, console, dbLog);
		
		Stream<Tweet> streamRet = StreamSupport.stream(iterator, false);
		return streamRet;
	}

	/**
	 * @return the generated stream of tweet
	 * @throws IOException if there was a problem accessing the file specified by the file option in platform.conf
	 * @throws SQLException if there was a problem conecting to the database
	 * @throws ClassNotFoundException if the mysql connector was not found 
	 */
	public Stream<Tweet> getStream() throws IOException, ClassNotFoundException, SQLException {
		if(config.isDemo()) { // offline stream from file
			return createOfflineStream();
		} else { // online stream from twitter
			System.out.println("starting online capturing");
			return createOnlineStream();
		}
	}

	/**
	 * Propagate the stop action to the internal iterator
	 * 
	 * @see twitter_filtering_stefano.StoppableSpliterator#stop()
	 */
	public void stop() {
		if(iterator != null) iterator.stop();
	}

	/**
	 * @return the instant of the first time window.
	 * 
	 * @see twitter_filtering_stefano.OfflineIterator#getFirstWindowStart()
	 */
	public Instant getFirstWindowStart() {
		return firstWindowStart;
	}

}
