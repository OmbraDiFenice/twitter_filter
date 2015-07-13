package twitter_filtering_stefano;

import java.util.ArrayList;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * Filter the stream of tweets using the configurations specified in filtering.conf.
 * 
 * The words in punteggiatura, stopWords, badWords and keywords (from capturing.conf) are 
 * removed from the tweet text, along with any link.
 * These words are always removed, even when they are embedded in other words.
 * 
 * When the tweet text contains any of the words in baseline, the entire tweet is 
 * flagged as discarded. This allow to easily remove the tweet from the stream.  
 * 
 * This class implements the {@link Function} interface, so it can be directly used as intermediate operation
 * in a stream.
 * 
 * @author stefano
 *
 */
public class Filtering implements Function<Tweet, Tweet>{

	private Pattern punteggiatura;
	private Pattern stopWords;
	private Pattern baseline;
	private Pattern badWords;
	private Pattern keywords;
	
	private MessageConsole console;
	
	public Filtering(Config config, MessageConsole console) {
		this.console = console;
		
		// initialize the regexp patterns to apply the filters.
		// the words in punteggiatura and keywords configuration options
		// will be filtered only when found as a whole in the twitter text, while the other ones are filtered
		// even if they are found as part of bigger words
		
		// always remove links (add link regexp to punteggiatura, so they will be automatically removed when punteggiatura filter is applied) 
		String linkRegExp = "\\b(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]\\b";
		String patternPunteggiatura = buildAlternativePattern(config.getPunteggiatura(), false);
		if(!patternPunteggiatura.equals("")) patternPunteggiatura += "|";
		patternPunteggiatura += linkRegExp;
		punteggiatura = Pattern.compile(patternPunteggiatura);
		
		stopWords = Pattern.compile(buildAlternativePattern(config.getStopWords(), true));
		baseline = Pattern.compile(buildAlternativePattern(config.getBaseline(), true));
		badWords = Pattern.compile(buildAlternativePattern(config.getBadWords(), true));
		keywords = Pattern.compile(buildAlternativePattern(config.getKeywords(), true));
	}
	
	/**
	 * Build a regexp string that matches any of the words in {@code list}. Passing {@code false}
	 * as {@code singleWord} the words are matched even if they are found as part of bigger words.
	 * Pass true to match the words as a whole.
	 * Note that the strings in {@code list} are assumed to be without spaces.
	 * 
	 * @param list list of words the returned regexp will match
	 * @param singleWord true if the words in list should be matched only as a whole
	 * @return a string that can be used to build a regular expression that will match any of the words in list
	 */
	private String buildAlternativePattern(ArrayList<String> list, boolean singleWord) {
		StringBuilder ret = new StringBuilder();
		for(String p : list) {
			if(!p.equals("")) {
				ret.append("|");
				if(singleWord) {
					ret.append("\\b");
					ret.append(Pattern.quote(p));
					ret.append("\\b");
				} else {
					ret.append(Pattern.quote(p));
				}
			}
		}
		if(ret.length() > 0) ret.deleteCharAt(0); // remove first '|'
		return ret.toString();
	}

	@Override
	public Tweet apply(Tweet tweet) {
		String text = tweet.getText();
		
		// remove any characters in punteggiatura, stopWords, badWords and keywords
		text = punteggiatura.matcher(text).replaceAll(" ");
		text = stopWords.matcher(text).replaceAll(" ");
		text = badWords.matcher(text).replaceAll(" ");
		text = keywords.matcher(text).replaceAll(" ");
		
		text = baseline.matcher(text).replaceAll(" ");
		
		// remove any multiple whitespaces and store the filtered text
		tweet.setText(text.replaceAll("\\s+", " ").trim());
		
//		// discard tweet if contains any of the words in baseline
//		if(!baseline.pattern().equals("") && baseline.matcher(text).find()) {
//			tweet.setDiscarded();
//			console.write("Tweet " + tweet.getId() + " discarded");
//		}
		
		return tweet;
	}
}
