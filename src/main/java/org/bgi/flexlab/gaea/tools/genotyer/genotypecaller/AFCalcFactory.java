package org.bgi.flexlab.gaea.tools.genotyer.genotypecaller;

import org.bgi.flexlab.gaea.data.exception.UserException;
import org.bgi.flexlab.gaea.tools.genotyer.VariantCallingEngine;
import org.bgi.flexlab.gaea.tools.mapreduce.genotyper.GenotyperOptions;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Factory to make AFCalculations
 */
public class AFCalcFactory {
    /**
     * Enumeration of usable AF calculation, their constraints (i.e. ploidy).
     *
     * Note that the order these occur in the enum is the order of preference, so
     * the first value is taken over the second when multiple calculations satisfy
     * the needs of the request (i.e., considering ploidy).
     */
    public enum Calculation {
        /** expt. implementation -- for testing only */
        EXACT_INDEPENDENT(IndependentAllelesDiploidExactAFCalc.class, 2, -1),

        /** reference implementation of multi-allelic EXACT model.  Extremely slow for many alternate alleles */
        EXACT_REFERENCE(ReferenceDiploidExactAFCalc.class, 2, -1),

        /** original biallelic exact model, for testing only */
        EXACT_ORIGINAL(OriginalDiploidExactAFCalc.class, 2, 2),

        /** implementation that supports any sample ploidy */
        EXACT_GENERAL_PLOIDY("GeneralPloidyExactAFCalc", -1, -1);

        /**
         * Must be a name because we look this up dynamically
         */
        public final String className;
        public final int maxAltAlleles;
        public final int requiredPloidy;

        private Calculation(final String className, final int requiredPloidy, final int maxAltAlleles) {
            this.className = className;
            this.requiredPloidy = requiredPloidy;
            this.maxAltAlleles = maxAltAlleles;
        }

        private Calculation(final Class clazz, final int requiredPloidy, final int maxAltAlleles) {
            this(clazz.getSimpleName(), requiredPloidy, maxAltAlleles);
        }

        public boolean usableForParams(final int requestedPloidy, final int requestedMaxAltAlleles) {
            return (requiredPloidy == -1 || requiredPloidy == requestedPloidy)
                    && (maxAltAlleles == -1 || maxAltAlleles >= requestedMaxAltAlleles);
        }

        public static Calculation getDefaultModel() { return EXACT_INDEPENDENT; }
    }

    private static final Map<String, Class<? extends AlleleFrequencyCalculator>> afClasses;
    static {
    	//FIXME: change it ,if it has other using .
        //afClasses = new PluginManager<AFCalc>(AFCalc.class).getPluginsByName();
    	afClasses=new HashMap<String, Class<? extends AlleleFrequencyCalculator>>();
    	afClasses.put("IndependentAllelesDiploidExact", IndependentAllelesDiploidExactAFCalc.class);
    	afClasses.put("OriginalDiploidExact", OriginalDiploidExactAFCalc.class);
    	afClasses.put("ReferenceDiploidExact", ReferenceDiploidExactAFCalc.class);
    }

    private AFCalcFactory() {

    }

    private static Class<? extends AlleleFrequencyCalculator> getClassByName(final String name) {
        for ( final Class<? extends AlleleFrequencyCalculator> clazz : afClasses.values() ) {
            if ( clazz.getSimpleName().contains(name) ) {
                return clazz;
            }
        }

        return null;
    }

    /**
     * Create a new AFCalc based on the parameters in the UAC
     *
     * @param options the UnifiedArgumentCollection containing the command-line parameters for the caller
     * @param nSamples the number of samples we will be using
     * @return an initialized AFCalc
     */
    public static AlleleFrequencyCalculator createAFCalc(final GenotyperOptions options, final int nSamples) {
        final int maxAltAlleles = options.getMaxAlternateAlleles();
        if ( ! options.getAFmodel().usableForParams(options.getSamplePloidy(), maxAltAlleles) ) {
            final List<Calculation> supportingCalculations = new LinkedList<Calculation>();
            for ( final Calculation calc : Calculation.values() ) {
                if ( calc.usableForParams(options.getSamplePloidy(), maxAltAlleles) )
                    supportingCalculations.add(calc);
            }

            if ( supportingCalculations.isEmpty() )
                throw new UserException("no AFCalculation model found that supports ploidy of " + options.getSamplePloidy() + " and max alt alleles " + maxAltAlleles);
            else if ( supportingCalculations.size() > 1 ){
            	
            }
            else
                options.setAFmodel(supportingCalculations.get(0));
        }

        final AlleleFrequencyCalculator calc = createAFCalc(options.getAFmodel(), nSamples, options.getMaxAlternateAlleles(), options.getSamplePloidy());

        //if ( UAC.exactCallsLog != null ) calc.enableProcessLog(UAC.exactCallsLog);

        return calc;
    }

    /**
     * Create a new AFCalc, choosing the best implementation based on the given parameters, assuming
     * that we will only be requesting bi-allelic variants to diploid genotypes
     *
     * @param nSamples the number of samples we'll be using
     *
     * @return an initialized AFCalc
     */
    public static AlleleFrequencyCalculator createAFCalc(final int nSamples) {
        return createAFCalc(chooseBestCalculation(nSamples, 2, 1), nSamples, 2, 2);
    }

    /**
     * Create a new AFCalc that supports maxAltAlleles for all variants and diploid genotypes
     *
     * @param calc the calculation we'd like to use
     * @param nSamples the number of samples we'll be using
     * @param maxAltAlleles the max. alt alleles for both SNPs and indels
     *
     * @return an initialized AFCalc
     */
    public static AlleleFrequencyCalculator createAFCalc(final Calculation calc, final int nSamples, final int maxAltAlleles) {
        return createAFCalc(calc, nSamples, maxAltAlleles, 2);
    }

    /**
     * Create a new AFCalc, choosing the best implementation based on the given parameters
     *
     * @param nSamples the number of samples we'll be using
     * @param maxAltAlleles the max. alt alleles to consider for SNPs
     * @param ploidy the sample ploidy.  Must be consistent with the calc
     *
     * @return an initialized AFCalc
     */
    public static AlleleFrequencyCalculator createAFCalc(final int nSamples, final int maxAltAlleles, final int ploidy) {
        return createAFCalc(chooseBestCalculation(nSamples, ploidy, maxAltAlleles), nSamples, maxAltAlleles, ploidy);
    }

    /**
     * Choose the best calculation for nSamples and ploidy
     *
     * @param nSamples
     * @param ploidy
     * @param maxAltAlleles
     * @return
     */
    private static Calculation chooseBestCalculation(final int nSamples, final int ploidy, final int maxAltAlleles) {
        for ( final Calculation calc : Calculation.values() ) {
            if ( calc.usableForParams(ploidy, maxAltAlleles) ) {
                return calc;
            }
        }

        throw new IllegalStateException("no calculation found that supports nSamples " + nSamples + " ploidy " + ploidy + " and maxAltAlleles " + maxAltAlleles);
    }

    /**
     * Create a new AFCalc
     *
     * @param calc the calculation to use
     * @param nSamples the number of samples we'll be using
     * @param maxAltAlleles the max. alt alleles to consider for SNPs
     * @param ploidy the sample ploidy.  Must be consistent with the calc
     *
     * @return an initialized AFCalc
     */
    public static AlleleFrequencyCalculator createAFCalc(final Calculation calc, final int nSamples, final int maxAltAlleles, final int ploidy) {
        if ( calc == null ) throw new IllegalArgumentException("Calculation cannot be null");
        if ( nSamples < 0 ) throw new IllegalArgumentException("nSamples must be greater than zero " + nSamples);
        if ( maxAltAlleles < 1 ) throw new IllegalArgumentException("maxAltAlleles must be greater than zero " + maxAltAlleles);
        if ( ploidy < 1 ) throw new IllegalArgumentException("sample ploidy must be greater than zero " + ploidy);

        if ( ! calc.usableForParams(ploidy, maxAltAlleles) )
            throw new IllegalArgumentException("AFCalc " + calc + " does not support requested ploidy " + ploidy);

        final Class<? extends AlleleFrequencyCalculator> afClass = getClassByName(calc.className);
        if ( afClass == null )
            throw new IllegalArgumentException("Unexpected AFCalc " + calc);

        try {
            Object args[] = new Object[]{nSamples, maxAltAlleles, ploidy};
            Constructor c = afClass.getDeclaredConstructor(int.class, int.class, int.class);
            return (AlleleFrequencyCalculator)c.newInstance(args);
        } catch (Exception e) {
            throw new UserException("Could not instantiate AFCalc " + calc, e);
        }
    }

    protected static List<AlleleFrequencyCalculator> createAFCalcs(final List<Calculation> calcs, final int nSamples, final int maxAltAlleles, final int ploidy) {
        final List<AlleleFrequencyCalculator> AFCalcs = new LinkedList<AlleleFrequencyCalculator>();

        for ( final Calculation calc : calcs )
            AFCalcs.add(createAFCalc(calc, nSamples, maxAltAlleles, ploidy));

        return AFCalcs;
    }
}
