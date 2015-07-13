package twitter_filtering_stefano;
import java.awt.Color;
import java.awt.Toolkit;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JScrollPane;
import javax.swing.JTextPane;
import javax.swing.text.BadLocationException;
import javax.swing.text.SimpleAttributeSet;
import javax.swing.text.StyleConstants;
import javax.swing.text.StyledDocument;

/**
 * Console-like window where messages can be reported to the user.
 * This console can be bound to a {@link Capturing} object, in which case
 * the {@link Capturing#stop()} message will be called when the console
 * window is closed.
 * 
 * @author stefano
 *
 */
public class MessageConsole extends WindowAdapter implements Log {

	// used to try to layout multiple windows nicely on the screen
	private static int lastY = 0;
	private static int lastX = 0;
	
	private Capturing capturing = null; // bound capturing object
	
	/**
	 * @param capturing the {@link Capturing} object to bind to this console
	 */
	public void setCapturing(Capturing capturing) {
		this.capturing = capturing;
	}

	private JFrame frame;
	
	private JTextPane output; // component where messages are reported
	private Color defaultColor; // default color, to be used when the write(String text) method without color specification is invoked 
	
	public MessageConsole(String title) {
		defaultColor = Color.yellow;
		
		output = new JTextPane();
		output.setDoubleBuffered(true);
		output.setBackground(Color.BLACK);
		output.setEditable(false);
		output.setFocusable(true);
        
		JScrollPane scrollPane = new JScrollPane(output);
        scrollPane.setFocusable(true);
        
        frame = new JFrame(title);
        frame.setDefaultCloseOperation(JDialog.DO_NOTHING_ON_CLOSE);
        frame.addWindowListener(this);
        frame.add(scrollPane);
        frame.setSize(500, 300);
        
        // try to nicely layout multiple windows on the screen
        frame.setLocation(lastX, lastY);
        lastY += 300;
        if(lastY + 300 > Toolkit.getDefaultToolkit().getScreenSize().getHeight()) {
        	lastY = 0;
        	lastX += 500;
        }
        
        frame.setVisible(true);
	}
	
	@Override
	public void write(String text) {
		write(text, defaultColor);
	}
	
	@Override
	public void write(String text, Color color) {
		if(output == null){
			System.out.println(text);
			return;
		}
		
		SimpleAttributeSet style = new SimpleAttributeSet();
		StyleConstants.setForeground(style, color);
		//output.setCharacterAttributes(style, true);
		
		if(!text.endsWith("\n")) text += "\n";
		
		//output.replaceSelection(text);
		StyledDocument doc = output.getStyledDocument();
		try {
			doc.insertString(0, text, style);
		} catch (BadLocationException e) {
			// ignore
			e.printStackTrace();
		}
	}

	@Override
	public void windowClosing(WindowEvent e) {
		if(capturing != null) capturing.stop();
		frame.dispose();
	}
}
