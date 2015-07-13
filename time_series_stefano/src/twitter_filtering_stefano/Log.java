package twitter_filtering_stefano;

import java.awt.Color;

/**
 * Simple interface enabling to report messages to the user
 * 
 * @author stefano
 *
 */
public interface Log {
	void write(String text);
	void write(String text, Color color);
}
