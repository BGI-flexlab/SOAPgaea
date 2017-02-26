package org.bgi.flexlab.gaea.tools.genotyer.annotator;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import htsjdk.variant.vcf.VCFStandardHeaderLines;
import org.bgi.flexlab.gaea.data.structure.alignment.AlignmentsBasic;
import org.bgi.flexlab.gaea.data.structure.header.VCFConstants;
import org.bgi.flexlab.gaea.data.structure.pileup.Mpileup;
import org.bgi.flexlab.gaea.data.structure.pileup.Pileup;
import org.bgi.flexlab.gaea.data.structure.pileup.PileupReadInfo;
import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformationShare;
import org.bgi.flexlab.gaea.data.structure.vcf.VariantDataTracker;
import org.bgi.flexlab.gaea.tools.genotyer.GenotypeLikelihoodCalculator.PerReadAlleleLikelihoodMap;
import org.bgi.flexlab.gaea.tools.genotyer.annotator.interfaces.ActiveRegionBasedAnnotation;
import org.bgi.flexlab.gaea.tools.genotyer.annotator.interfaces.InfoFieldAnnotation;
import org.bgi.flexlab.gaea.tools.genotyer.annotator.interfaces.StandardAnnotation;
import org.bgi.flexlab.gaea.util.MathUtils;
import org.bgi.flexlab.gaea.util.QualityUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Root Mean Square of the mapping quality of the reads across all samples.
 */
public class RMSMappingQuality extends InfoFieldAnnotation implements StandardAnnotation, ActiveRegionBasedAnnotation {

    public Map<String, Object> annotate(final VariantDataTracker tracker,
                                        final ChromosomeInformationShare ref,
                                        final Mpileup mpileup,
                                        final VariantContext vc,
                                        final Map<String, PerReadAlleleLikelihoodMap> perReadAlleleLikelihoodMap ) {
        int totalSize = 0, index = 0;
        int qualities[];
        if (mpileup != null) {
            if ( mpileup.getSize() == 0 )
                return null;
            totalSize = mpileup.totalDepth();

            qualities = new int[totalSize];

            for ( String sample : mpileup.getCurrentPosPileup().keySet() ) {
                Pileup pileup = mpileup.getCurrentPosPileup().get(sample);
                for (PileupReadInfo p : pileup.getPlp() )
                    index = fillMappingQualitiesFromPileupAndUpdateIndex(p.getReadInfo(), index, qualities);
            }
        }
        else if (perReadAlleleLikelihoodMap != null) {
            if ( perReadAlleleLikelihoodMap.size() == 0 )
                return null;

            for ( PerReadAlleleLikelihoodMap perReadLikelihoods : perReadAlleleLikelihoodMap.values() )
                totalSize += perReadLikelihoods.size();

            qualities = new int[totalSize];
            for ( PerReadAlleleLikelihoodMap perReadLikelihoods : perReadAlleleLikelihoodMap.values() ) {
                for (AlignmentsBasic read : perReadLikelihoods.getStoredElements())
                    index = fillMappingQualitiesFromPileupAndUpdateIndex(read, index, qualities);


        }
        }
        else
            return null;



        double rms = MathUtils.rms(qualities);
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(getKeyNames().get(0), String.format("%.2f", rms));
        return map;
    }

    private static int fillMappingQualitiesFromPileupAndUpdateIndex(final AlignmentsBasic read, final int inputIdx, final int[] qualities) {
        int outputIdx = inputIdx;
        if ( read.getMappingQual() != QualityUtils.MAPPING_QUALITY_UNAVAILABLE )
            qualities[outputIdx++] = read.getMappingQual();

        return outputIdx;
    }

    public List<String> getKeyNames() { return Arrays.asList(VCFConstants.RMS_MAPPING_QUALITY_KEY); }

    public List<VCFInfoHeaderLine> getDescriptions() {
        return Arrays.asList(VCFStandardHeaderLines.getInfoLine(getKeyNames().get(0)));
    }
}