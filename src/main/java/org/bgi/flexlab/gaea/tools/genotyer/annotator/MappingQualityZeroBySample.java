
package org.bgi.flexlab.gaea.tools.genotyer.annotator;


import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypeBuilder;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFormatHeaderLine;
import htsjdk.variant.vcf.VCFHeaderLineType;
import org.bgi.flexlab.gaea.data.structure.header.VCFConstants;
import org.bgi.flexlab.gaea.data.structure.pileup.Pileup;
import org.bgi.flexlab.gaea.data.structure.pileup.PileupReadInfo;
import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformationShare;
import org.bgi.flexlab.gaea.data.structure.vcf.VariantDataTracker;
import org.bgi.flexlab.gaea.tools.genotyer.genotypeLikelihoodCalculator.PerReadAlleleLikelihoodMap;
import org.bgi.flexlab.gaea.tools.genotyer.annotator.interfaces.GenotypeAnnotation;

import java.util.Arrays;
import java.util.List;

/**
 * Count for each sample of mapping quality zero reads
 */
public class MappingQualityZeroBySample extends GenotypeAnnotation {
    public void annotate(final VariantDataTracker tracker,
                         final ChromosomeInformationShare ref,
                         final Pileup pileup,
                         final VariantContext vc,
                         final Genotype g,
                         final GenotypeBuilder gb,
                         final PerReadAlleleLikelihoodMap alleleLikelihoodMap){
        if ( g == null || !g.isCalled() || pileup == null )
            return;

        int mq0 = 0;
        for (PileupReadInfo p : pileup.getPlp() ) {
            if ( p.getMappingQuality() == 0 )
                mq0++;
        }

        gb.attribute(getKeyNames().get(0), mq0);
    }

    public List<String> getKeyNames() { return Arrays.asList(VCFConstants.MAPPING_QUALITY_ZERO_KEY); }

    public List<VCFFormatHeaderLine> getDescriptions() { return Arrays.asList(
            new VCFFormatHeaderLine(getKeyNames().get(0), 1,
                    VCFHeaderLineType.Integer, "Number of Mapping Quality Zero Reads per sample")); }


}
