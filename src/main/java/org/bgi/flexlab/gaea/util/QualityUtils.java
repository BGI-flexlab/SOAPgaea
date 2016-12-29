package org.bgi.flexlab.gaea.util;

public class QualityUtils {
	public final static byte MAXIMUM_USABLE_QUALITY_SCORE = 93;
	public final static byte MINIMUM_USABLE_QUALITY_SCORE = 6;

	private static double qualityToErrorProbabilityCache[] = new double[256];
	static {
		for (int i = 0; i < 256; i++)
			qualityToErrorProbabilityCache[i] = qualityToErrorProbability(i);
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

	public static byte boundQuality(int qual) {
		return boundQuality(qual, MAXIMUM_USABLE_QUALITY_SCORE);
	}

	public static byte boundQuality(int qual, byte maxQual) {
		return (byte) Math.max(Math.min(qual, maxQual), 1);
	}
}
