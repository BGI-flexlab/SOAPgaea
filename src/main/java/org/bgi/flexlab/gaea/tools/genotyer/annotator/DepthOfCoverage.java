package org.bgi.flexlab.gaea.tools.genotyer.annotator;


import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import htsjdk.variant.vcf.VCFStandardHeaderLines;
import org.bgi.flexlab.gaea.data.structure.alignment.AlignmentsBasic;
import org.bgi.flexlab.gaea.data.structure.header.VCFConstants;
import org.bgi.flexlab.gaea.data.structure.pileup.Mpileup;
import org.bgi.flexlab.gaea.data.structure.pileup.Pileup;
import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformationShare;
import org.bgi.flexlab.gaea.data.structure.vcf.VariantDataTracker;
import org.bgi.flexlab.gaea.tools.genotyer.GenotypeLikelihoodCalculator.PerReadAlleleLikelihoodMap;
import org.bgi.flexlab.gaea.tools.genotyer.annotator.interfaces.ActiveRegionBasedAnnotation;
import org.bgi.flexlab.gaea.tools.genotyer.annotator.interfaces.InfoFieldAnnotation;
import org.bgi.flexlab.gaea.tools.genotyer.annotator.interfaces.StandardAnnotation;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Total (unfiltered) depth over all samples.
 *
 * While the sample-level (FORMAT) DP field describes the total depth of reads that passed the Unified Genotyper's
 * internal quality control metrics (like MAPQ > 17, for example), the INFO field DP represents the unfiltered depth
 * over all samples.  Note though that the DP is affected by downsampling (-dcov), so the max value one can obtain for
 * N samples with -dcov D is N * D
 */
public class DepthOfCoverage extends InfoFieldAnnotation implements StandardAnnotation, ActiveRegionBasedAnnotation {

    public Map<String, Object> annotate(final VariantDataTracker tracker,
                                        final ChromosomeInformationShare ref,
                                        final Mpileup mpileup,
                                        final VariantContext vc,
                                        final Map<String, PerReadAlleleLikelihoodMap> perReadAlleleLikelihoodMap ) {

        int depth = 0;
        if (mpileup != null) {
            if ( mpileup.getSize() == 0 )
                return null;

            for ( String sample : mpileup.getCurrentPosPileup().keySet() ) {
                Pileup pileup = mpileup.getCurrentPosPileup().get(sample);
                depth += pileup.depthOfCoverage();
            }
        }
        else if (perReadAlleleLikelihoodMap != null) {
            if ( perReadAlleleLikelihoodMap.size() == 0 )
                return null;

            for (PerReadAlleleLikelihoodMap maps : perReadAlleleLikelihoodMap.values() ) {
                for (Map.Entry<AlignmentsBasic, Map<Allele,Double>> el : maps.getLikelihoodReadMap().entrySet()) {
                    final AlignmentsBasic read = el.getKey();
                    depth += 1;
                }
            }
        }
        else
            return null;

        Map<String, Object> map = new HashMap<String, Object>();
        map.put(getKeyNames().get(0), String.format("%d", depth));
        return map;
    }

    public List<String> getKeyNames() { return Arrays.asList(VCFConstants.DEPTH_KEY); }

    public List<VCFInfoHeaderLine> getDescriptions() {
        return Arrays.asList(VCFStandardHeaderLines.getInfoLine(getKeyNames().get(0)));
    }
}
