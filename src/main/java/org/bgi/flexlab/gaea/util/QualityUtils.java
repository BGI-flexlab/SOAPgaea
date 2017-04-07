package org.bgi.flexlab.gaea.util;

public class QualityUtils {
	public final static byte MAXIMUM_USABLE_QUALITY_SCORE = 93;
	public final static byte MINIMUM_USABLE_QUALITY_SCORE = 6;
	public final static int MAPPING_QUALITY_UNAVAILABLE = 255;

	private static double qualityToErrorProbabilityCache[] = new double[256];
	public static double[] QUALITY_PROB = new double[MAXIMUM_USABLE_QUALITY_SCORE + 1];
	public static double[] QUALITY_PROB_LOG10 = new double[MAXIMUM_USABLE_QUALITY_SCORE + 1];
	public static double[] MINUS_QUALITY_PROB_LOG10 = new double[MAXIMUM_USABLE_QUALITY_SCORE + 1];

	static {
		for(byte quality = 0; quality < MAXIMUM_USABLE_QUALITY_SCORE + 1; quality++) {
			double errorProbability = qualityToErrorProbability(quality);
			QUALITY_PROB[quality] = errorProbability;
			QUALITY_PROB_LOG10[quality] = Math.log10(errorProbability);
			MINUS_QUALITY_PROB_LOG10[quality] = Math.log10(1 - errorProbability);
		}
	}

	static {
		for (int i = 0; i < 256; i++)
			qualityToErrorProbabilityCache[i] = qualityToErrorProbability(i);
	}

	static public double qualToProbLog10(byte qual) {
		return MINUS_QUALITY_PROB_LOG10[(int)qual & 0xff];
	}

	static public double qualToErrorProbLog10(byte qual) {
		return QUALITY_PROB_LOG10[(int)qual & 0xff]; // Map: 127 -> 127; -128 -> 128; -1 -> 255; etc.
	}

	public static double qualityToErrorProbability(final double qual) {
		return Math.pow(10.0, ((double) qual) / -10.0);
	}
	
	static public double qualityToProbability(double qual) {
        return 1.0 - qualityToErrorProbability(qual);
    }

	public static byte probabilityToQuality(double prob, double eps) {
		double lp = Math.round(-10.0 * Math.log10(1.0 - prob + eps));
		return boundQuality((int) lp);
	}

	public static double phredScaleErrorRate(double errorRate) {
		return Math.abs(-10.0 * Math.log10(errorRate));
	}

	public static byte boundQuality(int qual) {
		return boundQuality(qual, MAXIMUM_USABLE_QUALITY_SCORE);
	}

	public static byte boundQuality(int qual, byte maxQual) {
		return (byte) Math.max(Math.min(qual, maxQual), 1);
	}
}
