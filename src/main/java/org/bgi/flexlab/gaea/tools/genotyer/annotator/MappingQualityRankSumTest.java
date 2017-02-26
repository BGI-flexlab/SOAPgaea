package org.bgi.flexlab.gaea.tools.genotyer.annotator;


import htsjdk.samtools.SAMRecord;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import org.bgi.flexlab.gaea.data.structure.alignment.AlignmentsBasic;
import org.bgi.flexlab.gaea.data.structure.pileup.Pileup;
import org.bgi.flexlab.gaea.data.structure.pileup.PileupReadInfo;
import org.bgi.flexlab.gaea.tools.genotyer.GenotypeLikelihoodCalculator.PerReadAlleleLikelihoodMap;
import org.bgi.flexlab.gaea.tools.genotyer.annotator.interfaces.StandardAnnotation;

import java.util.Arrays;
import java.util.List;
import java.util.Map;


/**
 * The u-based z-approximation from the Mann-Whitney Rank Sum Test for mapping qualities (reads with ref bases vs. those with the alternate allele)
 * Note that the mapping quality rank sum test can not be calculated for sites without a mixture of reads showing both the reference and alternate alleles.
 */
public class MappingQualityRankSumTest extends RankSumTest implements StandardAnnotation {

    public List<String> getKeyNames() { return Arrays.asList("MQRankSum"); }

    public List<VCFInfoHeaderLine> getDescriptions() { return Arrays.asList(new VCFInfoHeaderLine("MQRankSum", 1, VCFHeaderLineType.Float, "Z-score From Wilcoxon rank sum test of Alt vs. Ref read mapping qualities")); }

    protected void fillQualsFromPileup(final List<Allele> allAlleles,
                                       final int refLoc,
                                       final Pileup pileup,
                                       final PerReadAlleleLikelihoodMap likelihoodMap,
                                       final List<Double> refQuals, final List<Double> altQuals) {

        if (pileup != null && likelihoodMap == null) {
            // old UG snp-only path through the annotations
            for ( final PileupReadInfo p : pileup.getPlp() ) {
                if ( isUsableBase(p) ) {
                    if ( allAlleles.get(0).equals(Allele.create((byte)p.getBase(), true)) ) {
                        refQuals.add((double)p.getMappingQuality());
                    } else if ( allAlleles.contains(Allele.create((byte)p.getBase()))) {
                        altQuals.add((double)p.getMappingQuality());
                    }
                }
            }
            return;
        }
        for (Map.Entry<AlignmentsBasic,Map<Allele,Double>> el : likelihoodMap.getLikelihoodReadMap().entrySet()) {
            final Allele a = PerReadAlleleLikelihoodMap.getMostLikelyAllele(el.getValue());
            // BUGBUG: There needs to be a comparable isUsableBase check here
            if (a.isNoCall())
                continue; // read is non-informative
            if (a.isReference())
                refQuals.add((double)el.getKey().getMappingQual());
            else if (allAlleles.contains(a))
                altQuals.add((double)el.getKey().getMappingQual());
        }
    }

 }