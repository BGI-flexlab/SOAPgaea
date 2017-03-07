
package org.bgi.flexlab.gaea.tools.genotyer.annotator;


import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypesContext;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import org.bgi.flexlab.gaea.data.structure.pileup.Mpileup;
import org.bgi.flexlab.gaea.data.structure.pileup.Pileup;
import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformationShare;
import org.bgi.flexlab.gaea.data.structure.vcf.VariantDataTracker;
import org.bgi.flexlab.gaea.tools.genotyer.genotypeLikelihoodCalculator.PerReadAlleleLikelihoodMap;
import org.bgi.flexlab.gaea.tools.genotyer.annotator.interfaces.InfoFieldAnnotation;
import org.bgi.flexlab.gaea.util.MathUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * The allele balance (fraction of ref bases over ref + alt bases) across all bialleleic het-called samples
 */
public class AlleleBalance extends InfoFieldAnnotation {


    char[] BASES = {'A','C','G','T'};
    public Map<String, Object> annotate(final VariantDataTracker tracker,
                                        final ChromosomeInformationShare ref,
                                        final Mpileup mpileup,
                                        final VariantContext vc,
                                        final Map<String, PerReadAlleleLikelihoodMap> stratifiedPerReadAlleleLikelihoodMap) {
        if ( mpileup.getSize() == 0 )
            return null;

        if ( !vc.isBiallelic() )
            return null;
        final GenotypesContext genotypes = vc.getGenotypes();
        if ( !vc.hasGenotypes() )
            return null;

        double ratioHom = 0.0;
        double ratioHet = 0.0;
        double weightHom = 0.0;
        double weightHet = 0.0;
        double overallNonDiploid = 0.0;
        for ( Genotype genotype : genotypes ) {
            // we care only about het calls

            Pileup pileup = mpileup.getCurrentPosPileup().get(genotype.getSampleName());
            if ( pileup == null )
                continue;

            if ( vc.isSNP() ) {
                final String bases = new String(pileup.getBases());
                if ( bases.length() == 0 )
                    return null;

                double pTrue = 1.0 - Math.pow(10.0,genotype.getLog10PError());
                if ( genotype.isHet() ) {
                    final char refChr = vc.getReference().toString().charAt(0);
                    final char altChr = vc.getAlternateAllele(0).toString().charAt(0);

                    final int refCount = MathUtils.countOccurrences(refChr, bases);
                    final int altCount = MathUtils.countOccurrences(altChr, bases);
                    final int otherCount = bases.length()-refCount-altCount;

                    // sanity check
                    if ( refCount + altCount == 0 )
                        continue;

                    // weight the allele balance by genotype quality so that e.g. mis-called homs don't affect the ratio too much
                    ratioHet += pTrue * ((double)refCount / (double)(refCount + altCount));
                    weightHet += pTrue;
                    overallNonDiploid += ( (double) otherCount )/(bases.length()*genotypes.size());
                } else if ( genotype.isHom() ) {
                    char alleleChr;
                    if ( genotype.isHomRef() ) {
                        alleleChr = vc.getReference().toString().charAt(0);
                    } else {
                        alleleChr = vc.getAlternateAllele(0).toString().charAt(0);
                    }
                    final int alleleCount = MathUtils.countOccurrences(alleleChr,bases);
                    int bestOtherCount = 0;
                    for ( char b : BASES ) {
                        if ( b == alleleChr )
                            continue;
                        int count = MathUtils.countOccurrences(b,bases);
                        if ( count > bestOtherCount )
                            bestOtherCount = count;
                    }
                    final int otherCount = bases.length() - alleleCount;
                    ratioHom += pTrue*( (double) alleleCount)/(alleleCount+bestOtherCount);
                    weightHom += pTrue;
                    overallNonDiploid += ((double ) otherCount)/(bases.length()*genotypes.size());
                }
                // Allele Balance for indels was not being computed correctly (since there was no allele matching).  Instead of
                // prolonging the life of imperfect code, I've decided to delete it.  If someone else wants to try again from
                // scratch, be my guest - but make sure it's done correctly!  [EB]
            }
        }

        // make sure we had a het genotype

        Map<String, Object> map = new HashMap<String, Object>();
        if ( weightHet > 0.0 ) {
            map.put("ABHet",ratioHet/weightHet);
        }

        if ( weightHom > 0.0 ) {
            map.put("ABHom",ratioHom/weightHom);
        }

        if ( overallNonDiploid > 0.0 ) {
            map.put("OND",overallNonDiploid);
        }
        return map;
    }


    public List<String> getKeyNames() { return Arrays.asList("ABHet","ABHom","OND"); }

    public List<VCFInfoHeaderLine> getDescriptions() { return Arrays.asList(new VCFInfoHeaderLine("ABHet", 1, VCFHeaderLineType.Float, "Allele Balance for hets (ref/(ref+alt))"),
            new VCFInfoHeaderLine("ABHom", 1, VCFHeaderLineType.Float, "Allele Balance for homs (A/(A+O))"),
            new VCFInfoHeaderLine("OND", 1, VCFHeaderLineType.Float, "Overall non-diploid ratio (alleles/(alleles+non-alleles))")); }
}
