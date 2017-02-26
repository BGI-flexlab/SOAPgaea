package org.bgi.flexlab.gaea.tools.genotyer.annotator;


import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import org.bgi.flexlab.gaea.data.structure.pileup.Mpileup;
import org.bgi.flexlab.gaea.data.structure.pileup.Pileup;
import org.bgi.flexlab.gaea.data.structure.pileup.PileupReadInfo;
import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformationShare;
import org.bgi.flexlab.gaea.data.structure.vcf.VariantDataTracker;
import org.bgi.flexlab.gaea.tools.genotyer.GenotypeLikelihoodCalculator.PerReadAlleleLikelihoodMap;
import org.bgi.flexlab.gaea.tools.genotyer.annotator.interfaces.ExperimentalAnnotation;
import org.bgi.flexlab.gaea.tools.genotyer.annotator.interfaces.InfoFieldAnnotation;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Fraction of all reads across samples that have mapping quality zero
 */
public class MappingQualityZeroFraction extends InfoFieldAnnotation implements ExperimentalAnnotation {

    public Map<String, Object> annotate(final VariantDataTracker tracker,
                                        final ChromosomeInformationShare ref,
                                        final Mpileup mpileup,
                                        final VariantContext vc,
                                        final Map<String, PerReadAlleleLikelihoodMap> stratifiedPerReadAlleleLikelihoodMap) {
        if ( mpileup.getSize() == 0 )
            return null;

        int mq0 = 0;
        int depth = 0;
        for ( String sample : mpileup.getCurrentPosPileup().keySet() ) {
            Pileup pileup = mpileup.getCurrentPosPileup().get(sample);
            depth += pileup.depthOfCoverage();
            for (PileupReadInfo p : pileup.getPlp() ) {
                if ( p.getMappingQuality() == 0 )
                    mq0++;
            }
        }
        if (depth > 0) {
            double mq0f = (double)mq0 / (double )depth;

            Map<String, Object> map = new HashMap<String, Object>();
            map.put(getKeyNames().get(0), String.format("%1.4f", mq0f));
            return map;
        }
        else
            return null;
    }

    public List<String> getKeyNames() { return Arrays.asList("MQ0Fraction"); }

    public List<VCFInfoHeaderLine> getDescriptions() { return Arrays.asList(new VCFInfoHeaderLine(getKeyNames().get(0), 1, VCFHeaderLineType.Integer, "Fraction of Mapping Quality Zero Reads")); }
}