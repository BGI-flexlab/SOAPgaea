package org.bgi.flexlab.gaea.util;

import java.math.RoundingMode;
import java.text.DecimalFormat;

public class MathUtils {

    public static DecimalFormat doubleformat = new DecimalFormat("0.000");
    public final static double LOG10_P_OF_ZERO = -1000000.0;

    /**
     * Private constructor.  No instantiating this class!
     */
    private MathUtils() {
    }
    
    
    static {
		doubleformat.setRoundingMode(RoundingMode.HALF_UP);
	}
    
    private static final double JACOBIAN_LOG_TABLE_STEP = 0.001;
    private static final double JACOBIAN_LOG_TABLE_INV_STEP = 1.0 / 0.001;
    private static final double MAX_JACOBIAN_TOLERANCE = 8.0;
    private static final int JACOBIAN_LOG_TABLE_SIZE = (int) (MAX_JACOBIAN_TOLERANCE / JACOBIAN_LOG_TABLE_STEP) + 1;
    private static final double[] jacobianLogTable;

    static {
        jacobianLogTable = new double[JACOBIAN_LOG_TABLE_SIZE];

        for (int k = 0; k < JACOBIAN_LOG_TABLE_SIZE; k++) {
            jacobianLogTable[k] = Math.log10(1.0 + Math.pow(10.0, -((double) k) * JACOBIAN_LOG_TABLE_STEP));
        }
    }

    public static int fastRound(double d) {
        return (d > 0.0) ? (int) (d + 0.5d) : (int) (d - 0.5d);
    }
    
    public static int sum(byte[] x) {
        int total = 0;
        for (byte v : x)
            total += (int)v;
        return total;
    }
    
    public static int[] addArrays(int[] a, int[] b) {
        int[] c = new int[a.length];
        for (int i = 0; i < a.length; i++)
            c[i] = a[i] + b[i];
        return c;
    }
    
    public static byte compareDoubles(double a, double b) {
        return compareDoubles(a, b, 1e-6);
    }

    public static byte compareDoubles(double a, double b, double epsilon) {
        if (Math.abs(a - b) < epsilon) {
            return 0;
        }
        if (a > b) {
            return -1;
        }
        return 1;
    }

    public static double distanceSquared(final double[] x, final double[] y) {
        double dist = 0.0;
        for (int iii = 0; iii < x.length; iii++) {
            dist += (x[iii] - y[iii]) * (x[iii] - y[iii]);
        }
        return dist;
    }

    static {
		doubleformat.setRoundingMode(RoundingMode.HALF_UP);
	}

    public static int maxElementIndex(final double[] array) {
        return maxElementIndex(array, array.length);
    }

    public static int maxElementIndex(final double[] array, final int endIndex) {
        if (array == null || array.length == 0)
            throw new IllegalArgumentException("Array cannot be null!");

        int maxI = 0;
        for (int i = 1; i < endIndex; i++) {
            if (array[i] > array[maxI])
                maxI = i;
        }

        return maxI;
    }


    public static double arrayMax(final double[] array) {
        return array[maxElementIndex(array)];
    }

    public static double arrayMax(final double[] array, final int endIndex) {
        return array[maxElementIndex(array, endIndex)];
    }

    /**
     * normalizes the log10-based array.  ASSUMES THAT ALL ARRAY ENTRIES ARE <= 0 (<= 1 IN REAL-SPACE).
     *
     * @param array             the array to be normalized
     * @param takeLog10OfOutput if true, the output will be transformed back into log10 units
     * @return a newly allocated array corresponding the normalized values in array, maybe log10 transformed
     */
    public static double[] normalizeFromLog10(double[] array, boolean takeLog10OfOutput) {
        return normalizeFromLog10(array, takeLog10OfOutput, false);
    }

    public static double[] normalizeFromLog10(double[] array, boolean takeLog10OfOutput, boolean keepInLogSpace) {
        // for precision purposes, we need to add (or really subtract, since they're
        // all negative) the largest value; also, we need to convert to normal-space.
        double maxValue = arrayMax(array);

        // we may decide to just normalize in log space without converting to linear space
        if (keepInLogSpace) {
            for (int i = 0; i < array.length; i++) {
                array[i] -= maxValue;
            }
            return array;
        }

        // default case: go to linear space
        double[] normalized = new double[array.length];

        for (int i = 0; i < array.length; i++)
            normalized[i] = Math.pow(10, array[i] - maxValue);

        // normalize
        double sum = 0.0;
        for (int i = 0; i < array.length; i++)
            sum += normalized[i];
        for (int i = 0; i < array.length; i++) {
            double x = normalized[i] / sum;
            if (takeLog10OfOutput) {
                x = Math.log10(x);
                if ( x < LOG10_P_OF_ZERO || Double.isInfinite(x) )
                    x = array[i] - maxValue;
            }

            normalized[i] = x;
        }

        return normalized;
    }

    /**
     * normalizes the log10-based array.  ASSUMES THAT ALL ARRAY ENTRIES ARE <= 0 (<= 1 IN REAL-SPACE).
     *
     * @param array the array to be normalized
     * @return a newly allocated array corresponding the normalized values in array
     */
    public static double[] normalizeFromLog10(double[] array) {
        return normalizeFromLog10(array, false);
    }

    public static double sumLog10(double[] log10values) {
        return Math.pow(10.0, log10sumLog10(log10values));
        //        double s = 0.0;
        //        for ( double v : log10values) s += Math.pow(10.0, v);
        //        return s;
    }

    public static double log10sumLog10(double[] log10values) {
        return log10sumLog10(log10values, 0);
    }


    public static double log10sumLog10(double[] log10p, int start) {
        return log10sumLog10(log10p, start, log10p.length);
    }

    public static double log10sumLog10(double[] log10p, int start, int finish) {
        double sum = 0.0;

        double maxValue = arrayMax(log10p, finish);
        if(maxValue == Double.NEGATIVE_INFINITY)
            return maxValue;

        for (int i = start; i < finish; i++) {
            sum += Math.pow(10.0, log10p[i] - maxValue);
        }

        return Math.log10(sum) + maxValue;
    }

    public static double approximateLog10SumLog10(double a, double b, double c) {
        return approximateLog10SumLog10(a, approximateLog10SumLog10(b, c));
    }

    public static double approximateLog10SumLog10(double small, double big) {
        // make sure small is really the smaller value
        if (small > big) {
            final double t = big;
            big = small;
            small = t;
        }

        if (small == Double.NEGATIVE_INFINITY || big == Double.NEGATIVE_INFINITY)
            return big;

        final double diff = big - small;
        if (diff >= MathUtils.MAX_JACOBIAN_TOLERANCE)
            return big;

        // OK, so |y-x| < tol: we use the following identity then:
        // we need to compute log10(10^x + 10^y)
        // By Jacobian logarithm identity, this is equal to
        // max(x,y) + log10(1+10^-abs(x-y))
        // we compute the second term as a table lookup with integer quantization
        // we have pre-stored correction for 0,0.1,0.2,... 10.0
        final int ind = fastRound(diff * MathUtils.JACOBIAN_LOG_TABLE_INV_STEP); // hard rounding
        return big + MathUtils.jacobianLogTable[ind];
    }
}
