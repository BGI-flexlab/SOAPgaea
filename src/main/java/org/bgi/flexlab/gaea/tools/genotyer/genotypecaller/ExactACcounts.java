package org.bgi.flexlab.gaea.tools.genotyer.genotypecaller;

import java.util.Arrays;

public final class ExactACcounts {
    private final int[] counts;
    private int hashcode = -1;

    public ExactACcounts(final int[] counts) {
        this.counts = counts;
    }

    public int[] getCounts() {
        return counts;
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof ExactACcounts) && Arrays.equals(getCounts(), ((ExactACcounts) obj).getCounts());
    }

    @Override
    public int hashCode() {
        if ( hashcode == -1 )
            hashcode = Arrays.hashCode(getCounts());
        return hashcode;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(getCounts()[0]);
        for ( int i = 1; i < getCounts().length; i++ ) {
            sb.append("/");
            sb.append(getCounts()[i]);
        }
        return sb.toString();
    }
}
