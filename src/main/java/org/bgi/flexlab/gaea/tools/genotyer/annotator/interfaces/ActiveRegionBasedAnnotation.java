package org.bgi.flexlab.gaea.tools.genotyer.annotator.interfaces;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import org.bgi.flexlab.gaea.tools.genotyer.GenotypeLikelihoodCalculator.PerReadAlleleLikelihoodMap;

import java.util.List;
import java.util.Map;

// TODO -- make this an abstract class when we move away from InfoFieldAnnotation
public interface ActiveRegionBasedAnnotation extends AnnotationType {
    // return annotations for the given contexts split by sample and then read likelihood
    public abstract Map<String, Object> annotate(final Map<String, PerReadAlleleLikelihoodMap> stratifiedContexts, final VariantContext vc);

    // return the descriptions used for the VCF INFO meta field
    public abstract List<VCFInfoHeaderLine> getDescriptions();
}