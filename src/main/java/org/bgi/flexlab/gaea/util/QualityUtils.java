package org.bgi.flexlab.gaea.util;

public class QualityUtils {
	public final static int MAXIMUM_USABLE_QUALITY_SCORE = 93;
	public final static int MINIMUM_USABLE_QUALITY_SCORE = 6;
	
	public static double qualityToErrorProbility(final double qual) {
		return Math.pow(10.0, ((double) qual) / -10.0);
	}
}
