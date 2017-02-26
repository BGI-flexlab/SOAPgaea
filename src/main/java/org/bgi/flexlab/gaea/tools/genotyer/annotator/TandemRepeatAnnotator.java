
package org.bgi.flexlab.gaea.tools.genotyer.annotator;


import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeaderLineCount;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import org.bgi.flexlab.gaea.data.structure.pileup.Mpileup;
import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformationShare;
import org.bgi.flexlab.gaea.data.structure.vcf.VariantDataTracker;
import org.bgi.flexlab.gaea.tools.genotyer.GenotypeLikelihoodCalculator.PerReadAlleleLikelihoodMap;
import org.bgi.flexlab.gaea.tools.genotyer.annotator.interfaces.InfoFieldAnnotation;
import org.bgi.flexlab.gaea.tools.genotyer.annotator.interfaces.StandardAnnotation;
import org.bgi.flexlab.gaea.util.GaeaVariantContextUtils;
import org.bgi.flexlab.gaea.util.Pair;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TandemRepeatAnnotator extends InfoFieldAnnotation implements StandardAnnotation {
    private static final String STR_PRESENT = "STR";
    private static final String REPEAT_UNIT_KEY = "RU";
    private static final String REPEATS_PER_ALLELE_KEY = "RPA";
    public Map<String, Object> annotate(final VariantDataTracker tracker,
                                        final ChromosomeInformationShare ref,
                                        final Mpileup mpileup,
                                        final VariantContext vc,
                                        final Map<String, PerReadAlleleLikelihoodMap> stratifiedPerReadAlleleLikelihoodMap) {
        if ( !vc.isIndel())
            return null;

        Pair<List<Integer>,byte[]> result = GaeaVariantContextUtils.getNumTandemRepeatUnits(vc, ref.getBaseBytes(Math.max(0, mpileup.getPosition() - 200), mpileup.getPosition() - 1));
        if (result == null)
            return null;

        byte[] repeatUnit = result.second;
        List<Integer> numUnits = result.first;

        Map<String, Object> map = new HashMap<String, Object>();
        map.put(STR_PRESENT,true);
        map.put(REPEAT_UNIT_KEY,new String(repeatUnit));
        map.put(REPEATS_PER_ALLELE_KEY, numUnits);

        return map;
    }

    protected static final String[] keyNames = {STR_PRESENT, REPEAT_UNIT_KEY,REPEATS_PER_ALLELE_KEY };
    protected static final VCFInfoHeaderLine[] descriptions = {
            new VCFInfoHeaderLine(STR_PRESENT, 0, VCFHeaderLineType.Flag, "Variant is a short tandem repeat"),
            new VCFInfoHeaderLine(REPEAT_UNIT_KEY, 1, VCFHeaderLineType.String, "Tandem repeat unit (bases)"),
            new VCFInfoHeaderLine(REPEATS_PER_ALLELE_KEY, VCFHeaderLineCount.UNBOUNDED, VCFHeaderLineType.Integer, "Number of times tandem repeat unit is repeated, for each allele (including reference)") };

    public List<String> getKeyNames() {
        return Arrays.asList(keyNames);
    }

    public List<VCFInfoHeaderLine> getDescriptions() { return Arrays.asList(descriptions); }

}
