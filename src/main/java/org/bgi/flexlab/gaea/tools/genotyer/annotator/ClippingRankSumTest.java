package org.bgi.flexlab.gaea.tools.genotyer.annotator;

import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import org.bgi.flexlab.gaea.data.structure.alignment.AlignmentsBasic;
import org.bgi.flexlab.gaea.data.structure.pileup.Pileup;
import org.bgi.flexlab.gaea.tools.genotyer.GenotypeLikelihoodCalculator.PerReadAlleleLikelihoodMap;

import java.util.Arrays;
import java.util.List;
import java.util.Map;


/**
 * Created with IntelliJ IDEA.
 * User: rpoplin
 * Date: 6/28/12
 */

/**
 * The u-based z-approximation from the Mann-Whitney Rank Sum Test for reads with clipped bases (reads with ref bases vs. those with the alternate allele)
 * Note that the clipping rank sum test can not be calculated for sites without a mixture of reads showing both the reference and alternate alleles.
 */
public class ClippingRankSumTest extends RankSumTest {

    public List<String> getKeyNames() { return Arrays.asList("ClippingRankSum"); }

    public List<VCFInfoHeaderLine> getDescriptions() { return Arrays.asList(new VCFInfoHeaderLine("ClippingRankSum", 1, VCFHeaderLineType.Float, "Z-score From Wilcoxon rank sum test of Alt vs. Ref number of hard clipped bases")); }


    protected void fillQualsFromPileup(final List<Allele> allAlleles,
                                       final int refLoc,
                                       final Pileup pileup,
                                       final PerReadAlleleLikelihoodMap likelihoodMap, final List<Double> refQuals, final List<Double> altQuals) {
        // todo - only support non-pileup case for now, e.g. active-region based version
        if (pileup != null || likelihoodMap == null)
            return;

        for (Map.Entry<AlignmentsBasic, Map<Allele,Double>> el : likelihoodMap.getLikelihoodReadMap().entrySet()) {

            final Allele a = PerReadAlleleLikelihoodMap.getMostLikelyAllele(el.getValue());
            if (a.isNoCall())
                continue; // read is non-informative
            if (a.isReference())
                refQuals.add((double)el.getKey().getNumHardClippedBases());
            else if (allAlleles.contains(a))
                altQuals.add((double)el.getKey().getNumHardClippedBases());

        }
    }

 }
