package org.bgi.flexlab.gaea.tools.genotyer.annotator;


import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import org.bgi.flexlab.gaea.data.structure.pileup.Pileup;
import org.bgi.flexlab.gaea.data.structure.pileup.PileupReadInfo;
import org.bgi.flexlab.gaea.tools.genotyer.genotypeLikelihoodCalculator.PerReadAlleleLikelihoodMap;
import org.bgi.flexlab.gaea.tools.genotyer.annotator.interfaces.StandardAnnotation;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * The u-based z-approximation from the Mann-Whitney Rank Sum Test for base qualities (ref bases vs. bases of the alternate allele).
 * Note that the base quality rank sum test can not be calculated for sites without a mixture of reads showing both the reference and alternate alleles.
 */
public class BaseQualityRankSumTest extends RankSumTest implements StandardAnnotation {
    public List<String> getKeyNames() { return Arrays.asList("BaseQRankSum"); }

    public List<VCFInfoHeaderLine> getDescriptions() { return Arrays.asList(new VCFInfoHeaderLine("BaseQRankSum", 1, VCFHeaderLineType.Float, "Z-score from Wilcoxon rank sum test of Alt Vs. Ref base qualities")); }

    protected void fillQualsFromPileup(final List<Allele> allAlleles, final int refLoc,
                                       final Pileup pileup,
                                       final PerReadAlleleLikelihoodMap alleleLikelihoodMap,
                                       final List<Double> refQuals, final List<Double> altQuals){

        if (alleleLikelihoodMap == null) {
            // use fast SNP-based version if we don't have per-read allele likelihoods
            for ( final PileupReadInfo p : pileup.getPlp() ) {
                if ( isUsableBase(p) ) {
                    if ( allAlleles.get(0).equals(Allele.create((byte)p.getBase(),true)) ) {
                        refQuals.add((double)p.getBaseQuality());
                    } else if ( allAlleles.contains(Allele.create((byte)p.getBase()))) {
                        altQuals.add((double)p.getBaseQuality());
                    }
                }
            }
            
            
            return;
        }

        for (Map<Allele,Double> el : alleleLikelihoodMap.getLikelihoodMapValues()) {
            final Allele a = PerReadAlleleLikelihoodMap.getMostLikelyAllele(el);
            if (a.isNoCall())
                continue; // read is non-informative
            if (a.isReference())
                refQuals.add(-10.0*(double)el.get(a));
            else if (allAlleles.contains(a))
                altQuals.add(-10.0*(double)el.get(a));


        }
       
    }


}