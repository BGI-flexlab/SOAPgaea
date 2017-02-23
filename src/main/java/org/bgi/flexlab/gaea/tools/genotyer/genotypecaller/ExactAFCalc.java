
package org.bgi.flexlab.gaea.tools.genotyer.genotypecaller;


import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypesContext;
import htsjdk.variant.variantcontext.VariantContextUtils;
import org.bgi.flexlab.gaea.util.GaeaVariantContextUtils;
import org.bgi.flexlab.gaea.util.MathUtils;

import java.util.ArrayList;

abstract class ExactAFCalc extends AlleleFrequencyCalculator {
    protected static final int HOM_REF_INDEX = 0;  // AA likelihoods are always first

    protected ExactAFCalc(final int nSamples, int maxAltAlleles, final int ploidy) {
        super(nSamples, maxAltAlleles, ploidy);
    }

    /**
     * Wrapper class that compares two likelihoods associated with two alleles
     */
    protected static final class LikelihoodSum implements Comparable<LikelihoodSum> {
        public double sum = 0.0;
        public Allele allele;

        public LikelihoodSum(Allele allele) { this.allele = allele; }

        public int compareTo(LikelihoodSum other) {
            final double diff = sum - other.sum;
            return ( diff < 0.0 ) ? 1 : (diff > 0.0 ) ? -1 : 0;
        }
    }

    /**
     * Unpack GenotypesContext into arraylist of doubel values
     * @param GLs            Input genotype context
     * @return               ArrayList of doubles corresponding to GL vectors
     */
    protected static ArrayList<double[]> getGLs(final GenotypesContext GLs, final boolean includeDummy) {
        ArrayList<double[]> genotypeLikelihoods = new ArrayList<double[]>(GLs.size() + 1);
        if ( includeDummy ) genotypeLikelihoods.add(new double[]{0.0,0.0,0.0}); // dummy
        for ( Genotype sample : GLs.iterateInSampleNameOrder() ) {
        	if ( sample.hasLikelihoods() ) {
                double[] gls = sample.getLikelihoods().getAsVector();
                if ( MathUtils.sum(gls) < GaeaVariantContextUtils.SUM_GL_THRESH_NOCALL )
                    genotypeLikelihoods.add(gls);
            }
        }

        return genotypeLikelihoods;
    }
}