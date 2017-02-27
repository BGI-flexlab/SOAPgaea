package org.bgi.flexlab.gaea.tools.genotyer.annotator;


import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import org.bgi.flexlab.gaea.data.structure.pileup.Mpileup;
import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformationShare;
import org.bgi.flexlab.gaea.data.structure.vcf.VariantDataTracker;
import org.bgi.flexlab.gaea.tools.genotyer.GenotypeLikelihoodCalculator.PerReadAlleleLikelihoodMap;
import org.bgi.flexlab.gaea.tools.genotyer.annotator.interfaces.ExperimentalAnnotation;
import org.bgi.flexlab.gaea.tools.genotyer.annotator.interfaces.InfoFieldAnnotation;
import org.bgi.flexlab.gaea.util.IndelUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Rough category of indel type (insertion, deletion, multi-allelic, other)
 */
public class IndelType extends InfoFieldAnnotation implements ExperimentalAnnotation {

    public Map<String, Object> annotate(final VariantDataTracker tracker,
                                        final ChromosomeInformationShare ref,
                                        final Mpileup mpileup,
                                        final VariantContext vc,
                                        final Map<String, PerReadAlleleLikelihoodMap> stratifiedPerReadAlleleLikelihoodMap) {

        int run;
        if (vc.isMixed()) {
            Map<String, Object> map = new HashMap<String, Object>();
            map.put(getKeyNames().get(0), String.format("%s", "MIXED"));
            return map;

        }
        else if ( vc.isIndel() ) {
            String type="";
            if (!vc.isBiallelic())
                type = "MULTIALLELIC_INDEL";
            else {
                if (vc.isSimpleInsertion())
                    type = "INS.";
                else if (vc.isSimpleDeletion())
                    type = "DEL.";
                else
                    type = "OTHER.";
                ArrayList<Integer> inds = IndelUtils.findEventClassificationIndex(vc, ref, mpileup.getPosition());
                for (int k : inds) {
                    type = type+ IndelUtils.getIndelClassificationName(k)+".";
                }
            }
            Map<String, Object> map = new HashMap<String, Object>();
            map.put(getKeyNames().get(0), String.format("%s", type));
            return map;

        } else {
            return null;
        }

    }

    public List<String> getKeyNames() { return Arrays.asList("IndelType"); }

    public List<VCFInfoHeaderLine> getDescriptions() { return Arrays.asList(new VCFInfoHeaderLine("IndelType", 1, VCFHeaderLineType.String, "Indel type description")); }

}
