package org.bgi.flexlab.gaea.tools.genotyer.annotator;


import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import org.bgi.flexlab.gaea.data.structure.pileup.Mpileup;
import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformationShare;
import org.bgi.flexlab.gaea.data.structure.vcf.VariantDataTracker;
import org.bgi.flexlab.gaea.tools.genotyer.GenotypeLikelihoodCalculator.PerReadAlleleLikelihoodMap;
import org.bgi.flexlab.gaea.tools.genotyer.annotator.interfaces.ExperimentalAnnotation;
import org.bgi.flexlab.gaea.tools.genotyer.annotator.interfaces.InfoFieldAnnotation;
import org.bgi.flexlab.gaea.util.BaseUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The GC content (# GC bases / # all bases) of the reference within 50 bp +/- this site
 */
public class GCContent extends InfoFieldAnnotation implements ExperimentalAnnotation {

    public Map<String, Object> annotate(final VariantDataTracker tracker,
                                        final ChromosomeInformationShare ref,
                                        final Mpileup mpileup,
                                        final VariantContext vc,
                                        final Map<String, PerReadAlleleLikelihoodMap> stratifiedPerReadAlleleLikelihoodMap) {
        double content = computeGCContent(ref, mpileup);
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(getKeyNames().get(0), String.format("%.2f", content));
        return map;
    }

    public List<String> getKeyNames() { return Arrays.asList("GC"); }

    public List<VCFInfoHeaderLine> getDescriptions() { return Arrays.asList(new VCFInfoHeaderLine("GC", 1, VCFHeaderLineType.Integer, "GC content within 20 bp +/- the variant")); }

    public boolean useZeroQualityReads() { return false; }

    private static double computeGCContent(ChromosomeInformationShare ref, Mpileup mpileup) {
        int gc = 0, at = 0;

        for ( byte base : ref.getBaseBytes(Math.max(0, mpileup.getPosition() - 50), Math.min(ref.getLength(), mpileup.getPosition() + 50)) ) {
            int baseIndex = BaseUtils.simpleBaseToBaseIndex(base);
            if ( baseIndex == BaseUtils.gIndex || baseIndex == BaseUtils.cIndex )
                gc++;
            else if ( baseIndex == BaseUtils.aIndex || baseIndex == BaseUtils.tIndex )
                at++;
            else
                ; // ignore
        }

        int sum = gc + at;
        return (100.0*gc) / (sum == 0 ? 1 : sum);
     }
}