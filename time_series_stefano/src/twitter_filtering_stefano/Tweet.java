package twitter_filtering_stefano;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Date;

/**
 * Encapsulates tweet informations
 * @author stefano
 *
 */
public class Tweet {
	private long id;
	private String text;
	private Instant timestamp;
	
	/**
	 * tweet timestamp format
	 */
	private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	/**
	 * true if the tweet has been flagged as discarded
	 */
	private boolean discarded = false;
	
	/**
	 * Initialize the tweet. The values are taken from {@code tweetLine},
	 * have to be separated by tabs and in the following order: id, text and timestamp.
	 * 
	 * @param tweetLine string containing the id, text and timestamp, separated by tabs
	 */
	public Tweet(String tweetLine) {
		String[] fields = tweetLine.split("\\t");
		id = Long.parseLong(fields[0]);
		text = fields[1].trim().replaceAll("^\"|\"$", "");
		String dateString = fields[2].trim().replaceAll("^\"|\"$", "");
		timestamp = parseTimestamp(dateString).toInstant();
	}
	
	public static Date parseTimestamp(String timestamp) {
		ParsePosition pp = new ParsePosition(0);
		Date date = dateFormat.parse(timestamp, pp);
		if(date == null) {
			throw new DateTimeParseException("error parsing timestamp: " + timestamp, timestamp, pp.getErrorIndex());
		}
		return date;
	}
	
	/**
	 * Build a tweet using the provided parameters
	 * 
	 * @param id tweet id
	 * @param text tweet text
	 * @param timestamp twweet timestamp
	 */
	public Tweet(Long id, String text, Instant timestamp) {
		this.id = id;
		this.text = text;
		this.timestamp = timestamp;
	}
	
	/**
	 * Copy constructor. Builds a new tweet having the same (copied) information of the argument.
	 * 
	 * @param tweet the object to clone
	 */
	public Tweet(Tweet tweet) {
		this.id = tweet.getId();
		this.text = tweet.getText();
		this.timestamp = tweet.getTimestamp().plusMillis(0); // copy object
	}

	// TODO perform character encoding conversion?
	
	/**
	 * Initialize the tweet with the provided values
	 * 
	 * @param id the tweet id
	 * @param text the tweet text
	 * @param timestamp the tweet timestamp
	 */
	public Tweet(long id, String text, Instant timestamp) {
		this.id = id;
		this.text = text;
		this.timestamp = timestamp;
	}

	/**
	 * @return the tweet id
	 */
	public long getId() {
		return id;
	}

	/**
	 * @return the tweet text
	 */
	public String getText() {
		return text;
	}
	
	/**
	 * Set the text of the tweet. This method is used during the filtering phase,
	 * when some words in the tweet are deleted.
	 * 
	 * @param text the new tweet text
	 */
	public void setText(String text) {
		this.text = text;
	}

	/**
	 * @return the tweet timestamp
	 */
	public Instant getTimestamp() {
		return timestamp;
	}
	
	/**
	 * @return the tweet timestamp as a formatted string
	 */
	public String getTimestampAsString() {
		return dateFormat.format(Date.from(timestamp));
	}

	/**
	 * @return true if the tweet has been flagged as discarded
	 */
	public boolean isDiscarded() {
		return discarded;
	}

	/**
	 * Flag the tweet as discarded
	 */
	public void setDiscarded() {
		this.discarded = true;
	}
	
	@Override
	public String toString() {
		return id + "\t" + text + "\t" + timestamp.toString() + (discarded ? " - discarded" : "");
	}

}
