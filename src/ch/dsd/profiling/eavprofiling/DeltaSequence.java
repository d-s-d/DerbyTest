package ch.dsd.profiling.eavprofiling;

/**
 * Created with IntelliJ IDEA.
 * User: dsd
 * Date: 5/22/13
 * Time: 2:56 PM
 */
public class DeltaSequence {
	private long startTick = 0;
	private long sum = 0;
	private double sumOfSquares = 0;
	private int n = 0;

	public void start() {
		startTick = System.currentTimeMillis();
	}

	public void stop() {
		final long delta = System.currentTimeMillis() - startTick;
		n++;
		sum += delta;
		sumOfSquares += ((double) (delta*delta));
	}

	public double getAverage() {
	  return ((double) sum) / ((double) n);
	}

	public double getStdDev() {
		double avg = getAverage();
		return Math.sqrt(sumOfSquares/((double) n) - (avg*avg));
	}

	public void clear() {
		sum = 0;
		sumOfSquares = 0;
		startTick = 0;
		n = 0;
	}
}
