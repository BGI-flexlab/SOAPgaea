package org.bgi.flexlab.gaea.util;

public class QualityUtils {
	public final static int MAXIMUM_USABLE_QUALITY_SCORE = 93;
	public final static int MINIMUM_USABLE_QUALITY_SCORE = 6;
	
	public static double qualityToErrorProbility(final double qual) {
		return Math.pow(10.0, ((double) qual) / -10.0);
	}
	
	static public double qualToProb(double qual) {
        return 1.0 - Math.pow(10.0, qual/(-10.0));
    }
}
