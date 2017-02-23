package org.bgi.flexlab.gaea.tools.genotyer;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMReadGroupRecord;
import htsjdk.variant.variantcontext.*;
import org.bgi.flexlab.gaea.data.structure.header.VCFConstants;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocation;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocationParser;
import org.bgi.flexlab.gaea.data.structure.pileup.Mpileup;
import org.bgi.flexlab.gaea.data.structure.pileup.ReadsPool;
import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformationShare;
import org.bgi.flexlab.gaea.data.structure.variant.VariantCallContext;
import org.bgi.flexlab.gaea.data.structure.vcf.VariantDataTracker;
import org.bgi.flexlab.gaea.tools.genotyer.GenotypeLikelihoodCalculator.GenotypeLikelihoodCalculator;
import org.bgi.flexlab.gaea.tools.genotyer.GenotypeLikelihoodCalculator.PerReadAlleleLikelihoodMap;
import org.bgi.flexlab.gaea.tools.genotyer.annotator.VariantAnnotatorEngine;
import org.bgi.flexlab.gaea.tools.genotyer.genotypecaller.AFCalcFactory;
import org.bgi.flexlab.gaea.tools.genotyer.genotypecaller.AFCalcResult;
import org.bgi.flexlab.gaea.tools.genotyer.genotypecaller.AlleleFrequencyCalculator;
import org.bgi.flexlab.gaea.tools.mapreduce.genotyper.GenotyperOptions;
import org.bgi.flexlab.gaea.util.GaeaVariantContextUtils;
import org.bgi.flexlab.gaea.util.MathUtils;
import org.bgi.flexlab.gaea.util.QualityUtils;

import java.util.*;

/**
 * Created by zhangyong on 2016/12/20.
 */
public class VariantCallingEngine {
    public static final String LOW_QUAL_FILTER_NAME = "LowQual";
    public static final String NUMBER_OF_DISCOVERED_ALLELES_KEY = "NDA";

    public enum OUTPUT_MODE {
        /** produces calls only at variant sites */
        EMIT_VARIANTS_ONLY,
        /** produces calls at variant sites and confident reference sites */
        EMIT_ALL_CONFIDENT_SITES,
        /** produces calls at any callable site regardless of confidence; this argument is intended only for point
         * mutations (SNPs) in DISCOVERY mode or generally when running in GENOTYPE_GIVEN_ALLELES mode; it will by
         * no means produce a comprehensive set of indels in DISCOVERY mode */
        EMIT_ALL_SITES
    }

    public static final double HUMAN_SNP_HETEROZYGOSITY = 1e-3;
    public static final double HUMAN_INDEL_HETEROZYGOSITY = 1e-4;

    // the standard filter to use for calls below the confidence threshold but above the emit threshold
    private static final Set<String> filter = new HashSet<String>(1);

    // because the allele frequency priors are constant for a given i, we cache the results to avoid having to recompute everything
    private final double[] log10AlleleFrequencyPriorsSNPs;
    private final double[] log10AlleleFrequencyPriorsIndels;

    /**
     * Mpileup struct
     */
    private Mpileup mpileup;

    /**
     * memory shared reference
     */
    private ChromosomeInformationShare reference;

    /**
     * genotype caller
     */
    private AlleleFrequencyCalculator alleleFrequencyCalculator;

    // the annotation engine
    private final VariantAnnotatorEngine annotationEngine;

    /**
     * options
     */
    private GenotyperOptions options;

    /**
     * genome location parser
     */
    private GenomeLocationParser genomeLocationParser;

    private Set<String> samples;

    private int N;


    /**
     * constructor
     * @param readsPool reads
     * @param windowStart window start
     * @param windowEnd window end
     */
    public VariantCallingEngine(ReadsPool readsPool, ChromosomeInformationShare reference, int windowStart, int windowEnd, GenotyperOptions options, SAMFileHeader samFileHeader) {
        mpileup = new Mpileup(readsPool, windowStart, windowEnd);
        this.reference = reference;
        this.options = options;
        genomeLocationParser = new GenomeLocationParser(samFileHeader.getSequenceDictionary());
        samples = new HashSet<>();
        for(SAMReadGroupRecord rg : samFileHeader.getReadGroups()) {
            samples.add(rg.getSample());
        }

        this.N = samples.size() * options.getSamplePloidy();
        log10AlleleFrequencyPriorsSNPs = new double[N+1];
        log10AlleleFrequencyPriorsIndels = new double[N+1];
        computeAlleleFrequencyPriors(N, log10AlleleFrequencyPriorsSNPs, options.getHeterozygosity());
        computeAlleleFrequencyPriors(N, log10AlleleFrequencyPriorsIndels, options.getIndelHeterozygosity());
        filter.add(LOW_QUAL_FILTER_NAME);

        annotationEngine = new VariantAnnotatorEngine();
    }

    public List<VariantContext> reduce() {
        List<VariantContext> vcList = new ArrayList<>();
        final Map<String, PerReadAlleleLikelihoodMap> perReadAlleleLikelihoodMap = new HashMap<>();
        for(GenotypeLikelihoodCalculator.Model model : GenotypeLikelihoodCalculator.modelsToUse) {
            VariantContext vc = GenotypeLikelihoodCalculator.glcm.get(model).genotypeLikelihoodCalculate(mpileup, reference, options, genomeLocationParser, perReadAlleleLikelihoodMap);
            vcList.add(vc);
        }

        return vcList;
    }

    public VariantCallContext calculateGenotypes(final VariantDataTracker tracker, final ChromosomeInformationShare reference, final VariantContext vc,
                                             final boolean inheritAttributesFromInputVC,
                                             final Map<String, PerReadAlleleLikelihoodMap> perReadAlleleLikelihoodMap) {

        boolean limitedContext = tracker == null || reference == null ;

        // initialize the data for this thread if that hasn't been done yet
        alleleFrequencyCalculator = AFCalcFactory.createAFCalc(options, N);

        // estimate our confidence in a reference call and return
        if (vc.getNSamples() == 0) {
            if (limitedContext)
                return null;
            return (options.getOutputMode() != VariantCallingEngine.OUTPUT_MODE.EMIT_ALL_SITES ?
                    estimateReferenceConfidence(vc, mpileup, getTheta(options.getGtlcalculators()), false, 1.0) :
                    generateEmptyContext(tracker, reference, mpileup.getPosition()));
        }
        AFCalcResult AFresult = alleleFrequencyCalculator.getLog10PNonRef(vc, getAlleleFrequencyPriors(options.getGtlcalculators()));

        // is the most likely frequency conformation AC=0 for all alternate alleles?
        boolean bestGuessIsRef = true;

        // determine which alternate alleles have AF>0
        final List<Allele> myAlleles = new ArrayList<Allele>(vc.getAlleles().size());
        final List<Integer> alleleCountsofMLE = new ArrayList<Integer>(vc.getAlleles().size());
        myAlleles.add(vc.getReference());
        for (int i = 0; i < AFresult.getAllelesUsedInGenotyping().size(); i++) {
            final Allele alternateAllele = AFresult.getAllelesUsedInGenotyping().get(i);
            if (alternateAllele.isReference())
                continue;

            // Compute if the site is considered polymorphic with sufficient confidence relative to our
            // phred-scaled emission QUAL
            final boolean isNonRef = AFresult.isPolymorphicPhredScaledQual(alternateAllele, options.getStandardConfidenceForEmitting());

            // if the most likely AC is not 0, then this is a good alternate allele to use
            if (isNonRef) {
                myAlleles.add(alternateAllele);
                alleleCountsofMLE.add(AFresult.getAlleleCountAtMLE(alternateAllele));
                bestGuessIsRef = false;
            }
        }
        final double PoFGT0 = Math.pow(10, AFresult.getLog10PosteriorOfAFGT0());

        // note the math.abs is necessary because -10 * 0.0 => -0.0 which isn't nice
        final double phredScaledConfidence =
                Math.abs(!bestGuessIsRef ? -10 * AFresult.getLog10PosteriorOfAFEq0() : -10 * AFresult.getLog10PosteriorOfAFGT0());

        // return a null call if we don't pass the confidence cutoff or the most likely allele frequency is zero

        if (options.getOutputMode() != VariantCallingEngine.OUTPUT_MODE.EMIT_ALL_SITES && !passesEmitThreshold(phredScaledConfidence, bestGuessIsRef)) {
            // technically, at this point our confidence in a reference call isn't accurately estimated
            //  because it didn't take into account samples with no data, so let's get a better estimate
            return limitedContext ? null : estimateReferenceConfidence(vc, mpileup, getTheta(options.getGtlcalculators()), true, PoFGT0);
        }
        // start constructing the resulting VC
        final GenomeLocation loc = genomeLocationParser.createGenomeLocation(vc);
        final VariantContextBuilder builder = new VariantContextBuilder("UG_call", loc.getContig(), loc.getStart(), loc.getStop(), myAlleles);
        builder.log10PError(phredScaledConfidence / -10.0);
        if (!passesCallThreshold(phredScaledConfidence))
            builder.filters(filter);

        // create the genotypes
        final GenotypesContext genotypes = alleleFrequencyCalculator.subsetAlleles(vc, myAlleles, true, options.getSamplePloidy());
        builder.genotypes(genotypes);

        // print out stats if we have a writer
        //if ( verboseWriter != null && !limitedContext )
        //    printVerboseData(refContext.getLocus().toString(), vc, PoFGT0, phredScaledConfidence, model);

        // *** note that calculating strand bias involves overwriting data structures, so we do that last
        final HashMap<String, Object> attributes = new HashMap<String, Object>();

        // inherit attributed from input vc if requested
        if (inheritAttributesFromInputVC)
            attributes.putAll(vc.getAttributes());
        // System.out.println("VCPUT:"+vc.toString());
        // if the site was downsampled, record that fact
        //FIXME::need to be add if possible
        //if (!limitedContext && rawContext.hasPileupBeenDownsampled())
        //    attributes.put(VCFConstants.DOWNSAMPLED_KEY, true);

        if (options.isAnnotateNumberOfAllelesDiscovered())
            attributes.put(NUMBER_OF_DISCOVERED_ALLELES_KEY, vc.getAlternateAlleles().size());

        // add the MLE AC and AF annotations
        if (alleleCountsofMLE.size() > 0) {
            attributes.put(VCFConstants.MLE_ALLELE_COUNT_KEY, alleleCountsofMLE);
            final int AN = builder.make().getCalledChrCount();
            final ArrayList<Double> MLEfrequencies = new ArrayList<Double>(alleleCountsofMLE.size());
            // the MLEAC is allowed to be larger than the AN (e.g. in the case of all PLs being 0, the GT is ./. but the exact model may arbitrarily choose an AC>1)
            for (int AC : alleleCountsofMLE)
                MLEfrequencies.add(Math.min(1.0, (double) AC / (double) AN));
            attributes.put(VCFConstants.MLE_ALLELE_FREQUENCY_KEY, MLEfrequencies);
        }
        // FIXME::remove cause this is barely used and add lots of coding and computing.
        // for(String k:attributes.keySet())
        // 	System.out.println(k);
        /*
        if (UAC.COMPUTE_SLOD && !limitedContext && !bestGuessIsRef) {
            //final boolean DEBUG_SLOD = false;

            // the overall lod
            //double overallLog10PofNull = AFresult.log10AlleleFrequencyPosteriors[0];
            double overallLog10PofF = AFresult.getLog10LikelihoodOfAFGT0();
            //if ( DEBUG_SLOD ) System.out.println("overallLog10PofF=" + overallLog10PofF);

            List<Allele> allAllelesToUse = builder.make().getAlleles();

            // the forward lod
            VariantContext vcForward = calculateLikelihoods(tracker, refContext, stratifiedContexts, AlignmentContextUtils.ReadOrientation.FORWARD, allAllelesToUse, false, options.getGtlcalculators(), perReadAlleleLikelihoodMap);
            //System.out.println("vcForward:"+vcForward.toString());
            AFresult = alleleFrequencyCalculator.getLog10PNonRef(vcForward, getAlleleFrequencyPriors(model));
            //double[] normalizedLog10Posteriors = MathUtils.normalizeFromLog10(AFresult.log10AlleleFrequencyPosteriors, true);
            double forwardLog10PofNull = AFresult.getLog10LikelihoodOfAFEq0();
            double forwardLog10PofF = AFresult.getLog10LikelihoodOfAFGT0();
            //if ( DEBUG_SLOD ) System.out.println("forwardLog10PofNull=" + forwardLog10PofNull + ", forwardLog10PofF=" + forwardLog10PofF);

            // the reverse lod
            VariantContext vcReverse = calculateLikelihoods(tracker, refContext, stratifiedContexts, AlignmentContextUtils.ReadOrientation.REVERSE, allAllelesToUse, false, options.getGtlcalculators(), perReadAlleleLikelihoodMap);
            //System.out.println("vcReverse:"+vcReverse.toString());
            AFresult = alleleFrequencyCalculator.getLog10PNonRef(vcReverse, getAlleleFrequencyPriors(model));
            //normalizedLog10Posteriors = MathUtils.normalizeFromLog10(AFresult.log10AlleleFrequencyPosteriors, true);
            double reverseLog10PofNull = AFresult.getLog10LikelihoodOfAFEq0();
            double reverseLog10PofF = AFresult.getLog10LikelihoodOfAFGT0();
            //if ( DEBUG_SLOD ) System.out.println("reverseLog10PofNull=" + reverseLog10PofNull + ", reverseLog10PofF=" + reverseLog10PofF);

            double forwardLod = forwardLog10PofF + reverseLog10PofNull - overallLog10PofF;
            double reverseLod = reverseLog10PofF + forwardLog10PofNull - overallLog10PofF;
            //if ( DEBUG_SLOD ) System.out.println("forward lod=" + forwardLod + ", reverse lod=" + reverseLod);

            // strand score is max bias between forward and reverse strands
            double strandScore = Math.max(forwardLod, reverseLod);
            // rescale by a factor of 10
            strandScore *= 10.0;
            //logger.debug(String.format("SLOD=%f", strandScore));

            if (!Double.isNaN(strandScore))
                attributes.put("SB", strandScore);
        }*/

        //System.out.println("======");
        // for(String k:attributes.keySet())
        // 	System.out.println(k);
        // finish constructing the resulting VC
        builder.attributes(attributes);
        VariantContext vcCall = builder.make();
        // if we are subsetting alleles (either because there were too many or because some were not polymorphic)
        // then we may need to trim the alleles (because the original VariantContext may have had to pad at the end).
        if (myAlleles.size() != vc.getAlleles().size() && !limitedContext) // limitedContext callers need to handle allele trimming on their own to keep their perReadAlleleLikelihoodMap alleles in sync
            vcCall = GaeaVariantContextUtils.reverseTrimAlleles(vcCall);

        //System.out.println("vcCall2:"+vcCall.toString());
        if (annotationEngine != null && !limitedContext) { // limitedContext callers need to handle annotations on their own by calling their own annotationEngine
            // Note: we want to use the *unfiltered* and *unBAQed* context for the annotations
            //final ReadBackedPileup pileup = rawContext.getBasePileup();
            //stratifiedContexts = AlignmentContextUtils.splitContextBySampleName(pileup);

            vcCall = annotationEngine.annotateContext(tracker, reference, mpileup, vcCall, perReadAlleleLikelihoodMap);
        }
        return new VariantCallContext(vcCall, confidentlyCalled(phredScaledConfidence, PoFGT0));
    }

    private VariantCallContext generateEmptyContext(VariantDataTracker tracker, ChromosomeInformationShare ref, int position) {
        VariantContext vc;

        // deal with bad/non-standard reference bases
        if ( !Allele.acceptableAlleleBases(new byte[]{(byte) ref.getBase(position)}) )
            return null;

        Set<Allele> alleles = new HashSet<Allele>();
        alleles.add(Allele.create((byte) ref.getBase(position), true));
        vc = new VariantContextBuilder("UG_call", ref.getChromosomeName(), position, position, alleles).make();

        if ( annotationEngine != null ) {
            // Note: we want to use the *unfiltered* and *unBAQed* context for the annotations
            //final ReadBackedPileup pileup = rawContext.getBasePileup();
            //stratifiedContexts = AlignmentContextUtils.splitContextBySampleName(pileup);

            vc = annotationEngine.annotateContext(tracker, ref, mpileup, vc);
        }

        return new VariantCallContext(vc, false);
    }

    private VariantCallContext estimateReferenceConfidence(VariantContext vc, Mpileup mpileup, double theta, boolean ignoreCoveredSamples, double initialPofRef) {
        if ( mpileup == null )
            return null;

        double P_of_ref = initialPofRef;

        // for each sample that we haven't examined yet
        for ( String sample : samples ) {
            boolean isCovered = mpileup.getCurrentPosPileup().containsKey(sample);
            if ( ignoreCoveredSamples && isCovered )
                continue;


            int depth = 0;

            if ( isCovered ) {
                depth = mpileup.getCurrentPosPileup().get(sample).depthOfCoverage();
            }

            P_of_ref *= 1.0 - (theta / 2.0) * getRefBinomialProb(depth);
        }

        return new VariantCallContext(vc, QualityUtils.phredScaleErrorRate(1.0 - P_of_ref) >= options.getStandardConfidenceForCalling(), false);
    }

    protected double getTheta( final GenotypeLikelihoodCalculator.Model model ) {
        if( model.name().contains("SNP") )
            return HUMAN_SNP_HETEROZYGOSITY;
        if( model.name().contains("INDEL") )
            return HUMAN_INDEL_HETEROZYGOSITY;
        else throw new IllegalArgumentException("Unexpected GenotypeCalculationModel " + model);
    }


    private final static double[] binomialProbabilityDepthCache = new double[10000];
    static {
        for ( int i = 1; i < binomialProbabilityDepthCache.length; i++ ) {
            binomialProbabilityDepthCache[i] = MathUtils.binomialProbability(0, i, 0.5);
        }
    }

    private final double getRefBinomialProb(final int depth) {
        if ( depth < binomialProbabilityDepthCache.length )
            return binomialProbabilityDepthCache[depth];
        else
            return MathUtils.binomialProbability(0, depth, 0.5);
    }

    protected double[] getAlleleFrequencyPriors( final GenotypeLikelihoodCalculator.Model model ) {
        if (model.name().toUpperCase().contains("SNP")){
            return log10AlleleFrequencyPriorsSNPs;
        }
        else if (model.name().toUpperCase().contains("INDEL")){
            return log10AlleleFrequencyPriorsIndels;
        }
        else
            throw new IllegalArgumentException("Unexpected GenotypeCalculationModel " + model);

    }

    public static void computeAlleleFrequencyPriors(final int N, final double[] priors, final double theta) {

        double sum = 0.0;

        // for each i
        for (int i = 1; i <= N; i++) {
            final double value = theta / (double)i;
            priors[i] = Math.log10(value);
            sum += value;
        }

        // null frequency for AF=0 is (1 - sum(all other frequencies))
        priors[0] = Math.log10(1.0 - sum);
    }

    protected boolean passesEmitThreshold(double conf, boolean bestGuessIsRef) {
        return (options.getOutputMode() == OUTPUT_MODE.EMIT_ALL_CONFIDENT_SITES || !bestGuessIsRef) && conf >= Math.min(options.getStandardConfidenceForCalling(), options.getStandardConfidenceForEmitting());
    }

    protected boolean passesCallThreshold(double conf) {
        return conf >= options.getStandardConfidenceForCalling();
    }

    protected boolean confidentlyCalled(double conf, double PofF) {
        return conf >= options.getStandardConfidenceForCalling();
    }

}
