package twitter_filtering_stefano;

import java.awt.Color;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.Map.Entry;

import wordcloud.CollisionMode;
import wordcloud.WordCloud;
import wordcloud.WordFrequency;
import wordcloud.bg.RectangleBackground;
import wordcloud.font.scale.LinearFontScalar;
import wordcloud.image.AngleGenerator;
import wordcloud.palette.ColorPalette;

/**
 * Apply some more filtering to the stream of tweets (relative to a single time window)
 * to remove words too short or having a too low frequency.
 * This class can then be used to generate the word cloud, both in txt form (as a list of
 * words and their frquency) and as a png image. 
 * 
 * @author stefano
 *
 */
public class Assessment {
	
	private Config conf;
	
	private MessageConsole console;
	
	/**
	 * will contain the top 'wordNumber' words having the highest frequency
	 */
	private ArrayList<WordFrequency> topWords = null;
	
	/**
	 * Id used to generate unique names for word cloud files
	 */
	private String uniqueID;
	
	public Assessment(Config conf, MessageConsole console) {
		this.conf = conf;
		this.console = console;
	}
	
	/**
	 * Filter the words in {@code frequencies} by the configured minimum frequency threshold and length.
	 */
	@SuppressWarnings("unchecked")
	public Assessment filterByConfigThresholds(Map<String, Long> frequencies) {
		topWords = new ArrayList<WordFrequency>(conf.getWordNumber());
		
		Arrays.stream(frequencies.entrySet().toArray(new Map.Entry[0]))
		.filter(entry -> ((Entry<String, Long>) entry).getValue() > conf.getFrequencyThreshold()) // keep words with frequency greater than the configured 'frequencyThreshold'
		.filter(entry -> ((Entry<String, Long>) entry).getKey().length() > conf.getMinWordLength()) // keep words having length greater than the configured 'minWordLength'
		.sorted((e1,  e2) -> (int) (((Entry<String, Long>) e2).getValue() - ((Entry<String, Long>) e1).getValue())) // sort in descending order of frequency
		.limit(conf.getWordNumber()) // take only the first 'wordNumber' words
		.forEach(entry -> topWords.add(new WordFrequency(((Entry<String, Long>)entry).getKey(), ((Entry<String, Long>) entry).getValue().intValue())));
		
		return this;
	}
	
	/**
	 * Generate the txt cloud file. This method should be called after {@link #filterByConfigThresholds(Map)}.
	 * 
	 * @return the assessment object itself. This allow to chain calls to other methods of this class
	 * @throws NullPointerException if this method is called before {@link #filterByConfigThresholds(Map)}
	 */
	public Assessment generateCloudText() {
		try {
			Files.createDirectories(Paths.get("frequencies"));
			
			try(PrintWriter out = 
					new PrintWriter(Files.newOutputStream(Paths.get("frequencies/word_frequency-" + uniqueID + ".txt"), StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE), true))		{
				for(WordFrequency word : topWords) {
					out.println(word.getWord() + " = " + word.getFrequency());
				}
				out.flush();
			} catch (IOException e) {
				console.write(e.getMessage(), Color.red);
			}
		} catch (IOException e1) {
			console.write(e1.getMessage(), Color.red);
		}
		
		return this;
	}
	
	/**
	 * Generate the png cloud file. This method should be called after {@link #filterByConfigThresholds(Map)}.
	 * 
	 * @return the assessment object itself. This allow to chain calls to other methods of this class
	 * @throws NullPointerException if this method is called before {@link #filterByConfigThresholds(Map)}
	 */
	public Assessment generateCloudImage() {
		WordCloud wordCloud = new WordCloud(conf.getCloudWidth(), conf.getCloudHeight(), CollisionMode.RECTANGLE);
		wordCloud.setPadding(1);
		wordCloud.setBackground(new RectangleBackground(conf.getCloudWidth(), conf.getCloudHeight()));
		wordCloud.setColorPalette(buildRandomColorPallete(40));
		wordCloud.setFontScalar(new LinearFontScalar(15, 50));
		wordCloud.setAngleGenerator(new AngleGenerator((int) conf.getAngleInclination()));
		
		wordCloud.build(topWords);
		
		try {
			Files.createDirectories(Paths.get("frequencies"));
			wordCloud.writeToFile("frequencies/tag_cloud-" + uniqueID + ".png");
		} catch (IOException e) {
			console.write(e.getMessage(), Color.red);
		}
		
		return this;
	}
	
	/**
	 * Generate a random color palette for the png word cloud. 
	 * 
	 * @param n number of different random colors
	 * @return the random color palette
	 */
	private ColorPalette buildRandomColorPallete(int n) {
        final Color[] colors = new Color[n];
        Random rand = new Random();
        for(int i = 0; i < colors.length; i++) {
            colors[i] = new Color(rand.nextInt(230) + 25, rand.nextInt(230) + 25, rand.nextInt(230) + 25);
        }
        return new ColorPalette(colors);
    }

	/**
	 * Set the unique ID for the cloud filenames. This id will be appended to the default file names.
	 * 
	 * Note that no check is performed on the uniqueness of the provided id
	 * 
	 * @param uniqueID id to be appended to the default file names
	 * @return the assessment object itself. This allow to chain calls to other methods of this class
	 */
	public Assessment setUniqueID(String uniqueID) {
		this.uniqueID = uniqueID;
		return this;
	}

}
