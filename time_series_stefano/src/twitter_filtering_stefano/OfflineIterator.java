package twitter_filtering_stefano;

import java.awt.Color;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.Instant;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.Spliterators.AbstractSpliterator;
import java.util.function.Consumer;
import java.util.regex.Pattern;

/**
 * Implement a {@link twitter_filtering_stefano.StoppableSpliterator} that iterate on the
 * tweets stored in a text file.
 * 
 * @author stefano
 *
 */
public class OfflineIterator extends StoppableSpliterator<Tweet> {
	private Scanner file;
	private Instant firstWindowStart = null; // time istant of the first tweet in the file
	private MessageConsole console;
	private DbWriter dbWriter; // parallel database accessing thread
	
	public OfflineIterator(Config config, MessageConsole console, MessageConsole dbConsole) throws IOException, ClassNotFoundException, SQLException {
		super(0, AbstractSpliterator.ORDERED | AbstractSpliterator.IMMUTABLE);
		
		this.console = console;
		
		String filename = config.getFile();
		
		Pattern.compile("(\\d+)\\t+\"(.{0,140})\"\\t+\"(.*)\"", Pattern.DOTALL);
		file = new Scanner(Files.newBufferedReader(Paths.get(filename), StandardCharsets.UTF_8));
		firstWindowStart = readTweet().getTimestamp();
		file = new Scanner(Files.newInputStream(Paths.get(filename))); // "rewind" the scanner
		
		// store tweets in the 'dbTable' table of the database specified in the configuration object.
		dbWriter = new DbWriter(config,"INSERT INTO `" + config.getDbTable() +"` VALUES (?,?,?)", dbConsole);
		dbWriter.start();
	}

	@Override
	public boolean tryAdvance(Consumer<? super Tweet> consumer) {
		if(stop) {
			dbWriter.finish();
			return false;
		}
		
		try {
			Tweet tweet = readTweet();
			
			// store the tweet in DB 'filtering' table
			// enqueue a copy of the tweet to avoid that the (asynchronous) query 
			// will insert modified (i.e. filtered) data in the database
			dbWriter.enqueue(new Tweet(tweet));
			
			console.write("analyzing tweet id: " + tweet.getId() + " timestamp: " + tweet.getTimestampAsString());
			
			consumer.accept(tweet);
			return true;
		} catch (NoSuchElementException e) {
			dbWriter.finish();
			return false;
		} catch(Exception e) {
			e.printStackTrace();
			console.write(e.getMessage(), Color.red);
			return false;
		}
	}
	
	/**
	 * Read the next tweet from the file
	 * 
	 * @return the read tweet
	 */
	private Tweet readTweet() {
		String tweetLine = "";
		
		// read the id and tweet text, taking into account that a tweet can contain newlines
		// and assuming that it will not contain tabs
		file.useDelimiter("\\t+");
		
		tweetLine += file.next().trim() + "\t";
		tweetLine += file.next().trim().toLowerCase() + "\t";
		
		// read the timestamp after the tweet, assuming that a tweet line is always 
		// terminated either with a line feed or a carriage return
		file.useDelimiter("\\n|\\r");
		tweetLine += file.next().trim();
		
		return new Tweet(tweetLine);
	}

	/**
	 * The timestamp of the first tweet stored in the file.
	 * 
	 * Note that the file is assumed to store in the first position the oldest tweet. 
	 * 
	 * @return the timestamp of the first tweet stored in the file
	 */
	public Instant getFirstWindowStart() {
		return firstWindowStart;
	}
	
}
