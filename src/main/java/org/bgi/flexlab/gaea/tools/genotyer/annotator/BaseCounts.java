
package org.bgi.flexlab.gaea.tools.genotyer.annotator;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import org.bgi.flexlab.gaea.data.structure.pileup.Mpileup;
import org.bgi.flexlab.gaea.data.structure.pileup.Pileup;
import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformationShare;
import org.bgi.flexlab.gaea.data.structure.vcf.VariantDataTracker;
import org.bgi.flexlab.gaea.tools.genotyer.genotypeLikelihoodCalculator.PerReadAlleleLikelihoodMap;
import org.bgi.flexlab.gaea.tools.genotyer.annotator.interfaces.InfoFieldAnnotation;
import org.bgi.flexlab.gaea.util.BaseUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Count of A, C, G, T bases across all samples
 */
public class BaseCounts extends InfoFieldAnnotation {

    public Map<String, Object> annotate(final VariantDataTracker tracker,
                                        final ChromosomeInformationShare ref,
                                        final Mpileup mpileup,
                                        final VariantContext vc,
                                        final Map<String, PerReadAlleleLikelihoodMap> stratifiedPerReadAlleleLikelihoodMap) {
        if ( mpileup.getSize() == 0 )
            return null;

        int[] counts = new int[4];

        for ( String sample : mpileup.getCurrentPosPileup().keySet() ) {
            Pileup pileup = mpileup.getCurrentPosPileup().get(sample);
            for (char base : pileup.getBases() ) {
                int index = BaseUtils.simpleBaseToBaseIndex((byte)base);
                if ( index != -1 )
                    counts[index]++;
            }
        }
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(getKeyNames().get(0), counts);
        return map;
    }

    public List<String> getKeyNames() { return Arrays.asList("BaseCounts"); }

    public List<VCFInfoHeaderLine> getDescriptions() { return Arrays.asList(new VCFInfoHeaderLine("BaseCounts", 4, VCFHeaderLineType.Integer, "Counts of each base")); }
}