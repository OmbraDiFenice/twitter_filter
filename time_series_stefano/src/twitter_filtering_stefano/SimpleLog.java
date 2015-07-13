package twitter_filtering_stefano;

import java.awt.Color;
import java.awt.Component;

import javax.swing.JOptionPane;

/**
 * Very basic implementation of the {@link Log} interface, showing any message
 * in a popup dialog box.
 * 
 * @author stefano
 *
 */
public class SimpleLog implements Log {

	private Component parent = null;
	
	public SimpleLog(Component parent) {
		this.parent = parent;
	}
	
	/**
	 * Show the message in a popup message box
	 */
	@Override
	public void write(String text) {
		JOptionPane.showMessageDialog(parent, text, "Message", JOptionPane.INFORMATION_MESSAGE);
	}

	/**
	 * This implementation ignores the specified text color
	 */
	@Override
	public void write(String text, Color color) {
		write(text);
	}

}
