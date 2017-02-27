package org.bgi.flexlab.gaea.tools.genotyer.annotator;

import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypeBuilder;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFormatHeaderLine;
import htsjdk.variant.vcf.VCFHeaderLineType;
import org.bgi.flexlab.gaea.data.structure.pileup.Pileup;
import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformationShare;
import org.bgi.flexlab.gaea.data.structure.vcf.VariantDataTracker;
import org.bgi.flexlab.gaea.tools.genotyer.GenotypeLikelihoodCalculator.PerReadAlleleLikelihoodMap;
import org.bgi.flexlab.gaea.tools.genotyer.annotator.interfaces.ExperimentalAnnotation;
import org.bgi.flexlab.gaea.tools.genotyer.annotator.interfaces.GenotypeAnnotation;
import org.bgi.flexlab.gaea.util.MathUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * The allele balance (fraction of ref bases over ref + alt bases) separately for each bialleleic het-called sample
 */
public class AlleleBalanceBySample extends GenotypeAnnotation implements ExperimentalAnnotation {

    public void annotate(final VariantDataTracker tracker,
                         final ChromosomeInformationShare ref,
                         final Pileup pileup,
                         final VariantContext vc,
                         final Genotype g,
                         final GenotypeBuilder gb,
                         final PerReadAlleleLikelihoodMap alleleLikelihoodMap){
        if ( pileup == null )
            return;

        Double ratio = annotateSNP(pileup, vc, g);
        if (ratio == null)
            return;

        gb.attribute(getKeyNames().get(0), Double.valueOf(String.format("%.2f", ratio.doubleValue())));
    }

    private Double annotateSNP(Pileup pileup, VariantContext vc, Genotype g) {
        double ratio = -1;

        if ( !vc.isSNP() )
            return null;

        if ( !vc.isBiallelic() )
            return null;

        if ( g == null || !g.isCalled() )
            return null;

        if (!g.isHet())
            return null;

        Collection<Allele> altAlleles = vc.getAlternateAlleles();
        if ( altAlleles.size() == 0 )
            return null;

        final String bases = new String(pileup.getBases());
        if ( bases.length() == 0 )
            return null;
        char refChr = vc.getReference().toString().charAt(0);
        char altChr = vc.getAlternateAllele(0).toString().charAt(0);

        int refCount = MathUtils.countOccurrences(refChr, bases);
        int altCount = MathUtils.countOccurrences(altChr, bases);

        // sanity check
        if ( refCount + altCount == 0 )
            return null;

        ratio = ((double)refCount / (double)(refCount + altCount));
        return ratio;
    }

    public List<String> getKeyNames() { return Arrays.asList("AB"); }

    public List<VCFFormatHeaderLine> getDescriptions() { return Arrays.asList(new VCFFormatHeaderLine(getKeyNames().get(0), 1, VCFHeaderLineType.Float, "Allele balance for each het genotype")); }
}