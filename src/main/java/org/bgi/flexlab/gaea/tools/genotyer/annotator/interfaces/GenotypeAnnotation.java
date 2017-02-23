package org.bgi.flexlab.gaea.tools.genotyer.annotator.interfaces;


import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypeBuilder;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFormatHeaderLine;
import org.bgi.flexlab.gaea.data.structure.pileup.Mpileup;
import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformationShare;
import org.bgi.flexlab.gaea.data.structure.vcf.VariantDataTracker;
import org.bgi.flexlab.gaea.tools.genotyer.GenotypeLikelihoodCalculator.PerReadAlleleLikelihoodMap;

import java.util.List;

public abstract class GenotypeAnnotation extends VariantAnnotatorAnnotation {

    // return annotations for the given contexts/genotype split by sample
    public abstract void annotate(final VariantDataTracker tracker,
                                  final AnnotatorCompatible walker,
                                  final ChromosomeInformationShare ref,
                                  final Mpileup mpileup,
                                  final VariantContext vc,
                                  final Genotype g,
                                  final GenotypeBuilder gb,
                                  final PerReadAlleleLikelihoodMap alleleLikelihoodMap);

    // return the descriptions used for the VCF FORMAT meta field
    public abstract List<VCFFormatHeaderLine> getDescriptions();

}