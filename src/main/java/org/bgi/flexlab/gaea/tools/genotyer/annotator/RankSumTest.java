package org.bgi.flexlab.gaea.tools.genotyer.annotator;


import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypesContext;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeaderLine;
import org.bgi.flexlab.gaea.data.structure.pileup.Mpileup;
import org.bgi.flexlab.gaea.data.structure.pileup.Pileup;
import org.bgi.flexlab.gaea.data.structure.pileup.PileupReadInfo;
import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformationShare;
import org.bgi.flexlab.gaea.data.structure.vcf.VariantDataTracker;
import org.bgi.flexlab.gaea.tools.genotyer.genotypeLikelihoodCalculator.PerReadAlleleLikelihoodMap;
import org.bgi.flexlab.gaea.tools.genotyer.annotator.interfaces.ActiveRegionBasedAnnotation;
import org.bgi.flexlab.gaea.tools.genotyer.annotator.interfaces.InfoFieldAnnotation;
import org.bgi.flexlab.gaea.util.MannWhitneyU;
import org.bgi.flexlab.gaea.util.Pair;
import org.bgi.flexlab.gaea.util.QualityUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Abstract root for all RankSum based annotations
 */
public abstract class RankSumTest extends InfoFieldAnnotation implements ActiveRegionBasedAnnotation {
    static final boolean DEBUG = false;
    private boolean useDithering = true;

    public Map<String, Object> annotate(final VariantDataTracker tracker,
                                        final ChromosomeInformationShare ref,
                                        final Mpileup mpileup,
                                        final VariantContext vc,
                                        final Map<String, PerReadAlleleLikelihoodMap> stratifiedPerReadAlleleLikelihoodMap) {
        // either stratifiedContexts or  stratifiedPerReadAlleleLikelihoodMap has to be non-null
    	
        final GenotypesContext genotypes = vc.getGenotypes();
        if (genotypes == null || genotypes.size() == 0)
            return null;

        final ArrayList<Double> refQuals = new ArrayList<Double>();
        final ArrayList<Double> altQuals = new ArrayList<Double>();

        for ( final Genotype genotype : genotypes.iterateInSampleNameOrder() ) {
            PerReadAlleleLikelihoodMap indelLikelihoodMap = null;

            if (stratifiedPerReadAlleleLikelihoodMap != null )
                indelLikelihoodMap = stratifiedPerReadAlleleLikelihoodMap.get(genotype.getSampleName());

            if (indelLikelihoodMap != null && indelLikelihoodMap.isEmpty())
                indelLikelihoodMap = null;
            // treat an empty likelihood map as a null reference - will simplify contract with fillQualsFromPileup
            if (indelLikelihoodMap == null && mpileup == null)
                continue;

            Pileup pileup = mpileup.getCurrentPosPileup().get(genotype.getSampleName());
            fillQualsFromPileup(vc.getAlleles(), vc.getStart(), pileup, indelLikelihoodMap, refQuals, altQuals );
        }
        
        if (refQuals.isEmpty() && altQuals.isEmpty())
            return null;

        final MannWhitneyU mannWhitneyU = new MannWhitneyU(useDithering);
        //System.out.println("useDithering:"+useDithering);
        for (final Double qual : altQuals) {
            mannWhitneyU.add(qual, MannWhitneyU.USet.SET1);
            //System.out.println("add alt:"+qual);
        }
        for (final Double qual : refQuals) {
            mannWhitneyU.add(qual, MannWhitneyU.USet.SET2);
            //System.out.println("add ref:"+qual);
        }

       /* System.out.format("%s, REF QUALS:", this.getClass().getName());
        for (final Double qual : refQuals)
            System.out.format("%4.1f ", qual);
        System.out.println();
        System.out.format("%s, ALT QUALS:", this.getClass().getName());
        for (final Double qual : altQuals)
            System.out.format("%4.1f ", qual);
        System.out.println();*/
        
        // we are testing that set1 (the alt bases) have lower quality scores than set2 (the ref bases)
        final Pair<Double, Double> testResults = mannWhitneyU.runOneSidedTest(MannWhitneyU.USet.SET1);
        //System.out.println(getKeyNames().get(0));
        //System.out.println("testResult:"+testResults.getFirst()+"\t"+testResults.getSecond());
        final Map<String, Object> map = new HashMap<String, Object>();
        if (!Double.isNaN(testResults.first))
            map.put(getKeyNames().get(0), String.format("%.3f", testResults.first));
        return map;
    }

     protected abstract void fillQualsFromPileup(final List<Allele> alleles,
                                                final int refLoc,
                                                final Pileup pileup,
                                                final PerReadAlleleLikelihoodMap alleleLikelihoodMap,
                                                final List<Double> refQuals,
                                                final List<Double> altQuals);

    /**
     * Can the base in this pileup element be used in comparative tests between ref / alt bases?
     *
     * Note that this function by default does not allow deletion pileup elements
     *
     * @param p the pileup element to consider
     * @return true if this base is part of a meaningful read for comparison, false otherwise
     */
    public static boolean isUsableBase(final PileupReadInfo p) {
        return isUsableBase(p, false);
    }

    /**
     * Can the base in this pileup element be used in comparative tests between ref / alt bases?
     *
     * @param p the pileup element to consider
     * @param allowDeletions if true, allow p to be a deletion base
     * @return true if this base is part of a meaningful read for comparison, false otherwise
     */
    public static boolean isUsableBase(final PileupReadInfo p, final boolean allowDeletions) {
        return !(p.isInsertionAtBeginningOfRead() ||
                 (! allowDeletions && p.isDeletionBase()) ||
                 p.getReadInfo().getMappingQual() == 0 ||
                p.getReadInfo().getMappingQual() == QualityUtils.MAPPING_QUALITY_UNAVAILABLE ||
                 ((int) p.getBaseQuality()) < QualityUtils.MINIMUM_USABLE_QUALITY_SCORE); // need the unBAQed quality score here
    }

    /**
     * Initialize the rank sum test annotation using walker and engine information. Right now this checks to see if
     * engine randomization is turned off, and if so does not dither.
     * @param headerLines
     */
    public void initialize ( Set<VCFHeaderLine> headerLines ) {
       // useDithering = ! toolkit.getArguments().disableRandomization;
    	useDithering=true;
    }
    
   
}