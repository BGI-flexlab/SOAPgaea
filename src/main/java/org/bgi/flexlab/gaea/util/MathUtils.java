package org.bgi.flexlab.gaea.util;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.Collection;

public class MathUtils {
    public static DecimalFormat doubleformat = new DecimalFormat("0.000");
    public final static double LOG10_P_OF_ZERO = -1000000.0;


    public static int fastRound(double d) {
        return (d > 0.0) ? (int) (d + 0.5d) : (int) (d - 0.5d);
    }
	
	/*
	 * add two array and return a new array
	 * */
	public static int[] addArrays(int[] a, int[] b) {
        int[] c = new int[a.length];
        for (int i = 0; i < a.length; i++){
            c[i] = a[i] + b[i];
        }
        return c;
    }
	
	public static double sum(Collection<? extends Number> numbers) {
        return sum(numbers, false);
    }

    public static double sum(Collection<? extends Number> numbers, boolean ignoreNan) {
        double sum = 0;
        for (Number n : numbers) {
            if (!ignoreNan || !Double.isNaN(n.doubleValue())) {
                sum += n.doubleValue();
            }
        }

        return sum;
    }
    
    public static int sum(byte[] x) {
        int total = 0;
        for (byte v : x){
            total += (int)v;
        }
        return total;
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

    /* public static double[] normalizeFromLog10(double[] array, boolean takeLog10OfOutput, boolean keepInLogSpace) {
         //true ,false
         // for precision purposes, we need to add (or really subtract, since they're
         // all negative) the largest value; also, we need to convert to normal-space.
         double maxValue = arrayMax(array);

         // we may decide to just normalize in log space without converting to linear space
         if (keepInLogSpace) {
             for (int i = 0; i < array.length; i++)
                 array[i] -= maxValue;
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
             if (takeLog10OfOutput)
                 x = Math.log10(x);
             normalized[i] = x;
         }

         return normalized;
     }*/
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
}
