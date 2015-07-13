package twitter_filtering_stefano;

import java.awt.Color;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

/**
 * Class providing access to the application configurations, defined in *.conf files.
 * Any configuration can be accessed by appropriate getter of this class, which returns
 * the corresponding value (of the correct type).
 * 
 * The {@link #store()} method can be used to store any modification made by call to setters
 * permanent to the configuration files.
 * 
 * @author stefano
 *
 */
public class Config {
	private Log console; // a place where display error messages
	
	private String platformFile = "platform.conf";
	private String capturingFile = "capturing.conf";
	private String filteringFile = "filtering.conf";
	private String assessmentFile = "assessment.conf";
	
	// actual properties
	
	// platform.conf
	private Properties platform;
	private boolean demo;
	private String file;
	private String dbAddress;
	private String dbUser;
	private String dbPassword;
	private String dbSchema;
	private String dbTable;
	
	// capturing.conf
	private Properties capturing;
	private WordList keywords;
	private boolean keepReply;
	private boolean keepRetweet;
	private String lng;
	private long duration;
	
	// filtering.conf (refer to former 'selection' phase)
	private Properties filtering;
	private WordList punteggiatura;
	private WordList stopWords;
	private WordList baseline;
	private WordList badWords;
	
	// assessment.conf
	private Properties assessment;
	private int minWordLength;
	private int frequencyThreshold;
	private int wordNumber;
	private int cloudWidth;
	private int cloudHeight;
	private double angleInclination;
	private int inclinationStep;
	private int refreshTime;
	
	/**
	 * Easily handle the conversion between space-separated list of words and its
	 * ArrayList<String> representation.
	 * 
	 * @author stefano
	 *
	 */
	@SuppressWarnings("serial")
	private class WordList extends ArrayList<String> {
		
		private String pattern = "\\s+";
		
		public WordList(String list) {
			if(list != null) {
				addAll(Arrays.asList(list.split(pattern)));
			}
		}
		
		@Override
		public String toString() {
			StringBuilder ret = new StringBuilder();
			for(String s : this) {
				ret.append(s + " ");
			}
			ret.deleteCharAt(ret.length()-1);
			return ret.toString();
		}
	}

	public Config(Log console) throws IOException {
		this.console = console;
		InputStream input = null;
				
		// load configurations from platform.conf
		platform = new Properties();
		input = new FileInputStream(platformFile);
		platform.load(input);
		
		demo = Boolean.parseBoolean(platform.getProperty("demo", "true"));
		file = platform.getProperty("file", "test.txt");
		dbAddress = platform.getProperty("dbAddress", "localhost");
		dbUser = platform.getProperty("dbUser", "root");
		dbPassword = platform.getProperty("dbPassword", "");
		dbSchema = platform.getProperty("dbSchema", "test");
		dbTable = platform.getProperty("dbTable", "time_series");
		
		input.close();
		
		// load configurations from capturing.conf
		capturing = new Properties();
		input = new FileInputStream(capturingFile);
		capturing.load(input);
		
		keywords = new WordList(capturing.getProperty("keywords", ""));
		keepReply = Boolean.parseBoolean(capturing.getProperty("keepReply", "false"));
		keepRetweet = Boolean.parseBoolean(capturing.getProperty("keepRetweet", "false"));
		lng = capturing.getProperty("lng", "it");
		duration = Long.parseLong(capturing.getProperty("duration", "60000"));
		
		input.close();

		// load configurations from filtering.conf
		filtering = new Properties();
		input = new FileInputStream(filteringFile);
		filtering.load(input);
		
		punteggiatura = new WordList(filtering.getProperty("punteggiatura", ""));
		stopWords = new WordList(filtering.getProperty("stopWords", ""));
		baseline = new WordList(filtering.getProperty("baseline", ""));
		badWords = new WordList(filtering.getProperty("badWords", ""));
		
		input.close();
		
		// load configurations from assessment.conf
		assessment = new Properties();
		input = new FileInputStream(assessmentFile);
		assessment.load(input);
		
		minWordLength = Integer.parseInt(assessment.getProperty("minWordLength", "4"));
		frequencyThreshold = Integer.parseInt(assessment.getProperty("frequencyThreshold", "15"));
		wordNumber = Integer.parseInt(assessment.getProperty("wordNumber", "20"));
		cloudWidth = Integer.parseInt(assessment.getProperty("cloudWidth", "600"));
		cloudHeight = Integer.parseInt(assessment.getProperty("cloudHeight", "200"));
		angleInclination = Double.parseDouble(assessment.getProperty("angleInclination", "0"));
		inclinationStep = Integer.parseInt(assessment.getProperty("inclinationStep", "0"));
		refreshTime = Integer.parseInt(assessment.getProperty("refreshTime", "60000"));
		
		input.close();
	}
	
	public long getDuration() {
		return duration;
	}

	private void store(String filename, Properties toSave) {
		try (OutputStream output = new FileOutputStream(filename)){
			toSave.store(output, filename);
			output.flush();
		} catch (IOException e) {
			console.write(e.getMessage(), Color.red);
		}
	}
	
	public void store() {
		// store platform configurations
		store(platformFile, platform);
		
		// store capturing configurations
		store(capturingFile, capturing);
		
		// store filtering configurations
		store(filteringFile, filtering);
		
		// store assessment configurations
		store(assessmentFile, assessment);
	}

	public boolean isDemo() {
		return demo;
	}

	public String getFile() {
		return file;
	}

	public String getDbAddress() {
		return dbAddress;
	}

	public String getDbUser() {
		return dbUser;
	}

	public String getDbPassword() {
		return dbPassword;
	}

	public String getDbSchema() {
		return dbSchema;
	}

	public String getDbTable() {
		return dbTable;
	}

	public ArrayList<String> getKeywords() {
		return keywords;
	}

	public boolean isKeepReply() {
		return keepReply;
	}

	public boolean isKeepRetweet() {
		return keepRetweet;
	}

	public String getLng() {
		return lng;
	}

	public ArrayList<String> getPunteggiatura() {
		return punteggiatura;
	}

	public ArrayList<String> getStopWords() {
		return stopWords;
	}

	public ArrayList<String> getBaseline() {
		return baseline;
	}

	public ArrayList<String> getBadWords() {
		return badWords;
	}

	public String getPlatform() {
		return platformFile;
	}

	public String getCapturing() {
		return capturingFile;
	}

	public String getFiltering() {
		return filteringFile;
	}

	public String getAssessment() {
		return assessmentFile;
	}

	public int getMinWordLength() {
		return minWordLength;
	}

	public int getFrequencyThreshold() {
		return frequencyThreshold;
	}

	public int getWordNumber() {
		return wordNumber;
	}

	public int getCloudWidth() {
		return cloudWidth;
	}

	public int getCloudHeight() {
		return cloudHeight;
	}

	public double getAngleInclination() {
		return angleInclination;
	}

	public int getInclinationStep() {
		return inclinationStep;
	}

	public void setDemo(boolean demo) {
		this.demo = demo;
		platform.setProperty("demo", String.valueOf(demo));
	}

	public void setFile(String file) {
		this.file = file;
		platform.setProperty("file", file);
	}

	public void setDbAddress(String dbAddress) {
		this.dbAddress = dbAddress;
		platform.setProperty("dbAddress", dbAddress);
	}

	public void setDbUser(String dbUser) {
		this.dbUser = dbUser;
		platform.setProperty("dbUser", dbUser);
	}

	public void setDbPassword(String dbPassword) {
		this.dbPassword = dbPassword;
		platform.setProperty("dbPassword", dbPassword);
	}

	public void setDbSchema(String dbSchema) {
		this.dbSchema = dbSchema;
		platform.setProperty("dbSchema", dbSchema);
	}

	public void setDbTable(String dbTable) {
		this.dbTable = dbTable;
		platform.setProperty("dbTable", dbTable);
	}

	public void setKeywords(String keywords) {
		this.keywords = new WordList(keywords);
		capturing.setProperty("keywords", this.keywords.toString());
	}

	public void setKeepReply(boolean keepReply) {
		this.keepReply = keepReply;
		capturing.setProperty("keepReply", String.valueOf(keepReply));
	}

	public void setKeepRetweet(boolean keepRetweet) {
		this.keepRetweet = keepRetweet;
		capturing.setProperty("keepRetweet", String.valueOf(keepRetweet));
	}

	public void setLng(String lng) {
		this.lng = lng;
		capturing.setProperty("lng", lng);
	}

	public void setDuration(long duration) {
		this.duration = duration;
		capturing.setProperty("duration", String.valueOf(duration));
	}

	public void setPunteggiatura(String punteggiatura) {
		this.punteggiatura = new WordList(punteggiatura);
		filtering.setProperty("punteggiatura", this.punteggiatura.toString());
	}

	public void setStopWords(String stopWords) {
		this.stopWords = new WordList(stopWords);
		filtering.setProperty("stopWords", this.stopWords.toString());
	}

	public void setBaseline(String baseline) {
		this.baseline = new WordList(baseline);
		filtering.setProperty("baseline", this.baseline.toString());
	}

	public void setBadWords(String badWords) {
		this.badWords = new WordList(badWords);
		filtering.setProperty("badWords", this.badWords.toString());
	}

	public void setMinWordLength(int minWordLength) {
		this.minWordLength = minWordLength;
		assessment.setProperty("minWordLength", String.valueOf(minWordLength));
	}

	public void setFrequencyThreshold(int frequencyThreshold) {
		this.frequencyThreshold = frequencyThreshold;
		assessment.setProperty("frequencyThreshold", String.valueOf(frequencyThreshold));
	}

	public void setWordNumber(int wordNumber) {
		this.wordNumber = wordNumber;
		assessment.setProperty("wordNumber", String.valueOf(wordNumber));
	}

	public void setCloudWidth(int cloudWidth) {
		this.cloudWidth = cloudWidth;
		assessment.setProperty("cloudWidth", String.valueOf(cloudWidth));
	}

	public void setCloudHeight(int cloudHeight) {
		this.cloudHeight = cloudHeight;
		assessment.setProperty("cloudHeight", String.valueOf(cloudHeight));
	}

	public void setAngleInclination(double angleInclination) {
		this.angleInclination = angleInclination;
		assessment.setProperty("angleInclination", String.valueOf(angleInclination));
	}

	public void setInclinationStep(int inclinationStep) {
		this.inclinationStep = inclinationStep;
		assessment.setProperty("inclinationStep", String.valueOf(inclinationStep));
	}

	public void setRefreshTime(int refreshTime) {
		this.refreshTime = refreshTime;
		assessment.setProperty("refreshTime", String.valueOf(refreshTime));
	}

	public int getRefreshTime() {
		return refreshTime;
	}
}
