package org.bgi.flexlab.gaea.tools.genotyer.annotator.interfaces;


import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import org.bgi.flexlab.gaea.data.structure.pileup.Mpileup;
import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformationShare;
import org.bgi.flexlab.gaea.data.structure.vcf.VariantDataTracker;
import org.bgi.flexlab.gaea.tools.genotyer.genotypeLikelihoodCalculator.PerReadAlleleLikelihoodMap;

import java.util.List;
import java.util.Map;

public abstract class InfoFieldAnnotation extends VariantAnnotatorAnnotation {
    // return annotations for the given contexts split by sample
    public Map<String, Object> annotate(final VariantDataTracker tracker,
                                        final ChromosomeInformationShare ref,
                                        final Mpileup mpileup,
                                        final VariantContext vc) {
        return annotate(tracker, ref, mpileup, vc, null);
    }

    public Map<String, Object> annotate(Map<String, PerReadAlleleLikelihoodMap> perReadAlleleLikelihoodMap, VariantContext vc) {
        return annotate(null, null, null, vc, perReadAlleleLikelihoodMap);
    }


    public abstract Map<String, Object> annotate(final VariantDataTracker tracker,
                                                 final ChromosomeInformationShare ref,
                                                 final Mpileup mpileup,
                                                 final VariantContext vc,
                                                 final Map<String, PerReadAlleleLikelihoodMap> stratifiedPerReadAlleleLikelihoodMap);

    // return the descriptions used for the VCF INFO meta field
    public abstract List<VCFInfoHeaderLine> getDescriptions();
}