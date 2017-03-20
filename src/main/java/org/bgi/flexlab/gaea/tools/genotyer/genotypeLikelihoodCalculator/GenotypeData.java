package org.bgi.flexlab.gaea.tools.genotyer.genotypeLikelihoodCalculator;

import org.bgi.flexlab.gaea.util.BaseUtils;
import org.bgi.flexlab.gaea.util.DiploidGenotype;

import static org.apache.commons.math3.util.FastMath.log10;


/**
 * Created by zhangyong on 2016/12/27.
 */
public class GenotypeData {

    /**
     * Constant static data: genotype zeros
     */
    public final static double[] genotypeZeros = new double[DiploidGenotype.values().length];

    /**
     * Constant static data: base zeros
     */
    public final static double[] baseZeros = new double[BaseUtils.BASES.length];

    static {
        for ( DiploidGenotype g : DiploidGenotype.values() ) {
            genotypeZeros[g.ordinal()] = 0.0;
        }
        for ( byte base : BaseUtils.BASES ) {
            baseZeros[BaseUtils.simpleBaseToBaseIndex(base)] = 0.0;
        }
    }

    /**
     * The fundamental data arrays associated with a Genotype Likelihoods object
     */
    protected double[] log10Likelihoods = null;

    public GenotypeData() {
        setToZero();
    }

    protected void setToZero() {
        log10Likelihoods = genotypeZeros.clone();                 // likelihoods are all zeros
    }

    public void add(GenotypeData genotypeData) {
        for(int i = 0; i < log10Likelihoods.length; i++) {
            log10Likelihoods[i] += genotypeData.log10Likelihoods[i];
        }
    }

    public double[] getLog10Likelihoods() {
        return log10Likelihoods;
    }
}
