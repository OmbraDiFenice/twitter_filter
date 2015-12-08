package twitter_filtering_stefano;


import java.awt.Color;
import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/**
 * This is the main class, where the computation is started.
 * 
 * @author stefano
 *
 */
public class CloudGenerator {

	public static void main(String[] args) {
		Instant start = Instant.now();
		
		MessageConsole filtering = new MessageConsole("Filtering");
		try {
			Config conf = new Config(filtering);
			new CloudGenerator().generate(conf, filtering, new MessageConsole("Database interaction"));
		} catch(IOException e) {
			filtering.write(e.getMessage(), Color.red);
		}
		
		Instant stop = Instant.now();
		System.out.println("elapsed time: " + Duration.between(start, stop));
	}
	
	/**
	 * Start the stream of tweet (either from file or live from twitter), 
	 * perform the filtering and produce the word clouds for each recognized time window
	 * 
	 * @param conf the application configuration object
	 * @param filteringLog the main console where to report any message concerning the filtering/cloud generation process
	 * @param dbLog the console where messages from database interaction are reported
	 */
	public void generate(Config conf, MessageConsole filteringLog, MessageConsole dbLog) {
		Capturing capturing = new Capturing(conf, filteringLog, dbLog);
		Filtering filtering = new Filtering(conf, filteringLog);
//		Assessment assessment = new Assessment(conf, filteringLog);
		
		filteringLog.setCapturing(capturing);
		dbLog.setCapturing(capturing);
		
		try {
			// apply the filters specified in filtering.conf
			// and create the time windows by grouping tweets in the same time interval.
			// The time interval of a window is specified by the refreshTime option in assessment.conf
			
			Map<Instant, List<Tweet>> timeWindows = capturing.getStream() // take the stream of tweets. This will be from file if 'demo' is true in platform.conf, otherwise from twitter
				.map(filtering) // apply the filter to tweet text and flag as discarded if appropriate
				.filter((Tweet tweet) -> !tweet.isDiscarded()) // remove flagged tweets from the stream
				// group together tweets belonging to the same time window and output a Map having the time window 
				// initial instant as key and the list of tweets as values 
				.collect(Collectors.groupingBy((Tweet tweet ) -> { // TODO ci sono modi più efficienti per suddividere i tweet
					Instant i = capturing.getFirstWindowStart();
					Instant last = i;
					Instant tweetTimestamp = tweet.getTimestamp(); 
					while(i.isBefore(tweetTimestamp)) {
						last = i;
						i = i.plusMillis(conf.getRefreshTime());
					}
					return last;
				}));
			
			filteringLog.write(timeWindows.size() + " time windows generated. Starting assessment...", Color.green);
			
			// for each time window, apply the filters specified in assessment.conf
			// and generate the tag cloud
			
			SimpleDateFormat formatter = new SimpleDateFormat("HHmmss"); // timestamp id formatter

//			// serial execution of assessment for each time window
//			for(Map.Entry<Instant, List<Tweet>> window : timeWindows.entrySet()) {
//				// create the unique id for the output files using the window starting instant
//				Date timestamp = Date.from(window.getKey());
//				String id = formatter.format(timestamp);
//				
//				filteringLog.write("processing time window " + id + "...");
//				
//				Map<String, Long> frequencies = window.getValue().stream() // take the stream of tweets in this time window list
//					.flatMap(tweet -> Arrays.asList(tweet.getText().split("\\s+")).stream()) // explode each tweet text into a stream of words
//					.collect(Collectors.groupingBy((String word) -> word, Collectors.mapping((String word) -> word, Collectors.counting()))); // "mapreduce" the stream of words into a Map<String, Long> associating each words to its frequency, return the Map
//				
//				// further filter on minimum word length and frequency
//				// and generate the tag cloud for the current time window
//				assessment
//					.setUniqueID(id) // has to be called before the 'generate' methods
//					.filterByConfigThresholds(frequencies) // apply the minimum length and frequency filters
//					.generateCloudText() // generate the cloud txt file
//					.generateCloudImage(); // generate the cloud image
//				
//				filteringLog.write("done processing time window " + id);
//			}
//			
//			filteringLog.write("done.", Color.green);
			
			// execute the assessment task in parallel for all the time windows
			@SuppressWarnings("unchecked")
			Map.Entry<Instant, List<Tweet>> timeWindowsArray[] = (Entry<Instant, List<Tweet>>[]) timeWindows.entrySet().toArray(new Map.Entry[0]);
			Thread worker[] = new Thread[timeWindowsArray.length];
			for(int i = 0; i < timeWindowsArray.length; i++) {
				int j = i;
				worker[i] = new Thread(() -> {
					// create the unique id for the output files using the window starting instant
					Date timestamp = Date.from(timeWindowsArray[j].getKey());
					String id = formatter.format(timestamp);
					
					filteringLog.write("processing time window " + id + "...");
					
					Map<String, Long> frequencies = timeWindowsArray[j].getValue().stream() // take the stream of tweets in this time window list
						.flatMap(tweet -> Arrays.asList(tweet.getText().split("\\s+")).stream()) // explode each tweet text into a stream of words
						.collect(Collectors.groupingBy((String word) -> word, Collectors.mapping((String word) -> word, Collectors.counting()))); // "mapreduce" the stream of words into a Map<String, Long> associating each words to its frequency, return the Map
					
					// further filter on minimum word length and frequency
					// and generate the tag cloud for the current time window
					new Assessment(conf, filteringLog)
						.setUniqueID(id) // has to be called before the 'generate' methods
						.filterByConfigThresholds(frequencies) // apply the minimum length and frequency filters
						.generateCloudText() // generate the cloud txt file
						.generateCloudImage(); // generate the cloud image
					
					filteringLog.write("done processing time window " + id);
				});
				worker[i].start();
			}
			
			// wait for all the assessment task to complete
			try {
				for(int i = 0; i < worker.length; i++) {
					worker[i].join();
				}
				filteringLog.write("done.", Color.green);
			} catch(InterruptedException e) {
				filteringLog.write(e.getMessage(), Color.red);
			}
		
		} catch (IOException e) {
			filteringLog.write(e.getMessage(), Color.red);
		} catch (ClassNotFoundException e) {
			filteringLog.write(e.getMessage(), Color.red);
		} catch (SQLException e) {
			filteringLog.write(e.getMessage(), Color.red);
		}
		
		conf.store();
	}
}
