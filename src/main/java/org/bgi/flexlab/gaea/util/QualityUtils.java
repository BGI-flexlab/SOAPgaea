package org.bgi.flexlab.gaea.util;

public class QualityUtils {
	public final static byte MAXIMUM_USABLE_QUALITY_SCORE = 93;
	public final static byte MINIMUM_USABLE_QUALITY_SCORE = 6;

	public static double[] QUALITY_PROB = new double[MAXIMUM_USABLE_QUALITY_SCORE + 1];
	public static double[] QUALITY_PROB_LOG10 = new double[MAXIMUM_USABLE_QUALITY_SCORE + 1];
	public static double[] MINUS_QUALITY_PROB_LOG10 = new double[MAXIMUM_USABLE_QUALITY_SCORE + 1];

	static {
		for(byte quality = 0; quality < MAXIMUM_USABLE_QUALITY_SCORE + 1; quality++) {
			QUALITY_PROB[quality] = qualityToErrorProbility(quality);
			QUALITY_PROB_LOG10[quality] = Math.log(qualityToErrorProbility(quality));
			MINUS_QUALITY_PROB_LOG10[quality] = Math.log(1 - qualityToErrorProbility(quality));
		}
	}

	private static double qualityToErrorProbilityCache[] = new double[256];
	static {
		for (int i = 0; i < 256; i++)
			qualityToErrorProbilityCache[i] = qualityToErrorProbility(i);
	}

	public static double qualityToErrorProbility(final double qual) {
		return Math.pow(10.0, ((double) qual) / -10.0);
	}

	public static byte probilityToQuality(double prob, double eps) {
		double lp = Math.round(-10.0 * Math.log10(1.0 - prob + eps));
		return boundQuality((int) lp);
	}

	public static byte boundQuality(int qual) {
		return boundQuality(qual, MAXIMUM_USABLE_QUALITY_SCORE);
	}

	public static byte boundQuality(int qual, byte maxQual) {
		return (byte) Math.max(Math.min(qual, maxQual), 1);
	}
}
