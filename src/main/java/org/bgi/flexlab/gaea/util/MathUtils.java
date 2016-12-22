package org.bgi.flexlab.gaea.util;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.Collection;

public class MathUtils {
    public static DecimalFormat doubleformat = new DecimalFormat("0.000");

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
    
    /**
     * Compares double values for equality (within epsilon), or inequality.
     *
     * @param a       the first double value
     * @param b       the second double value
     * @param epsilon the precision within which two double values will be considered equal
     * @return -1 if a is greater than b, 0 if a is equal to be within epsilon, 1 if b is greater than a.
     */
    public static byte compareDoubles(double a, double b, double epsilon) {
        if (Math.abs(a - b) < epsilon) {
            return 0;
        }
        if (a > b) {
            return -1;
        }
        return 1;
    }
    
	static {
		doubleformat.setRoundingMode(RoundingMode.HALF_UP);
	}
}
