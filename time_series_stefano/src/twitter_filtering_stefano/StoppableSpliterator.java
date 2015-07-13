package twitter_filtering_stefano;

import java.util.Spliterators.AbstractSpliterator;

/**
 * Spliterator exposing a {{@link #stop()} method, allowing to stop the 
 * iteration. Subclasses are in charge of correctly implement the
 * stop action.
 * 
 * @author stefano
 *
 * @param <T>
 */
public abstract class StoppableSpliterator<T> extends AbstractSpliterator<T> {
	
	protected boolean stop = false;

	protected StoppableSpliterator(long est, int additionalCharacteristics) {
		super(est, additionalCharacteristics);
	}

	/**
	 * Set the stop flag for this iterator. Subclasses are in charge of implementing
	 * the appropriate reaction to this event.
	 */
	public void stop() {
		stop = true;
	}
}