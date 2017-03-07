package org.bgi.flexlab.gaea.tools.genotyer.annotator;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import org.bgi.flexlab.gaea.data.structure.pileup.Mpileup;
import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformationShare;
import org.bgi.flexlab.gaea.data.structure.vcf.VariantDataTracker;
import org.bgi.flexlab.gaea.tools.genotyer.genotypeLikelihoodCalculator.PerReadAlleleLikelihoodMap;
import org.bgi.flexlab.gaea.tools.genotyer.annotator.interfaces.InfoFieldAnnotation;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Largest contiguous homopolymer run of the variant allele in either direction on the reference.  Computed only for bi-allelic sites.
 */
public class HomopolymerRun extends InfoFieldAnnotation {

    private boolean ANNOTATE_INDELS = true;

    public Map<String, Object> annotate(final VariantDataTracker tracker,
                                        final ChromosomeInformationShare ref,
                                        final Mpileup mpileup,
                                        final VariantContext vc,
                                        final Map<String, PerReadAlleleLikelihoodMap> stratifiedPerReadAlleleLikelihoodMap) {

        if ( !vc.isBiallelic() )
            return null;

        int run;
        if ( vc.isSNP() ) {
            run = computeHomopolymerRun(vc.getAlternateAllele(0).getBases()[0], ref, mpileup.getPosition());
        } else if ( vc.isIndel() && ANNOTATE_INDELS ) {
            run = computeIndelHomopolymerRun(vc,ref, mpileup.getPosition());
        } else {
            return null;
        }
        
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(getKeyNames().get(0), String.format("%d", run));
        return map;
    }

    public List<String> getKeyNames() { return Arrays.asList("HRun"); }

    public List<VCFInfoHeaderLine> getDescriptions() { return Arrays.asList(new VCFInfoHeaderLine("HRun", 1, VCFHeaderLineType.Integer, "Largest Contiguous Homopolymer Run of Variant Allele In Either Direction")); }

    public boolean useZeroQualityReads() { return false; }

    private static int computeHomopolymerRun(byte altAllele, ChromosomeInformationShare ref, int position) {

        // TODO -- this needs to be computed in a more accurate manner
        // We currently look only at direct runs of the alternate allele adjacent to this position



        int leftRun = 0;
        for (int i = position - 1; i >= Math.max(0, position - 200); i--) {
            if ( ref.getBase(i) != altAllele )
                break;
            leftRun++;
        }

        int rightRun = 0;
        for ( int i = position + 1; i < position + 200; i++) {
            if ( ref.getBase(i) != altAllele )
                break;
            rightRun++;
        }

        return Math.max(leftRun, rightRun);
     }

    private static int computeIndelHomopolymerRun(VariantContext vc, ChromosomeInformationShare ref, int position) {

        if ( vc.isSimpleDeletion() ) {
            // check that deleted bases are the same
            byte dBase = (byte)ref.getBase(position);
            for ( int i = 0; i < vc.getReference().length(); i ++ ) {
                if ( ref.getBase(i + position) != dBase ) {
                    return 0;
                }
            }

            return computeHomopolymerRun(dBase, ref, position);
        } else {
            // check that inserted bases are the same
            byte insBase = vc.getAlternateAllele(0).getBases()[0];
            for ( byte b : vc.getAlternateAllele(0).getBases() ) {
                if ( insBase != (char) b ) {
                    return 0;
                }
            }

            return computeHomopolymerRun(insBase,ref, position);
        }
    }
}