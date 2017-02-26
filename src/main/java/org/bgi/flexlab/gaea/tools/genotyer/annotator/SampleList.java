
package org.bgi.flexlab.gaea.tools.genotyer.annotator;

import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeaderLineCount;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import org.bgi.flexlab.gaea.data.structure.pileup.Mpileup;
import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformationShare;
import org.bgi.flexlab.gaea.data.structure.vcf.VariantDataTracker;
import org.bgi.flexlab.gaea.tools.genotyer.GenotypeLikelihoodCalculator.PerReadAlleleLikelihoodMap;
import org.bgi.flexlab.gaea.tools.genotyer.annotator.interfaces.InfoFieldAnnotation;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * List all of the polymorphic samples.
 */
public class SampleList extends InfoFieldAnnotation {

    public Map<String, Object> annotate(final VariantDataTracker tracker,
                                        final ChromosomeInformationShare ref,
                                        final Mpileup mpileup,
                                        final VariantContext vc,
                                        final Map<String, PerReadAlleleLikelihoodMap> stratifiedPerReadAlleleLikelihoodMap) {
        if ( vc.isMonomorphicInSamples() || !vc.hasGenotypes() )
            return null;

        StringBuffer samples = new StringBuffer();
        for ( Genotype genotype : vc.getGenotypesOrderedByName() ) {
            if ( genotype.isCalled() && !genotype.isHomRef() ){
                if ( samples.length() > 0 )
                    samples.append(",");
                samples.append(genotype.getSampleName());
            }
        }

        if ( samples.length() == 0 )
            return null;

        Map<String, Object> map = new HashMap<String, Object>();
        map.put("Samples", samples.toString());
        return map;
    }

    public List<String> getKeyNames() { return Arrays.asList("Samples"); }

    public List<VCFInfoHeaderLine> getDescriptions() { return Arrays.asList(new VCFInfoHeaderLine("Samples", VCFHeaderLineCount.UNBOUNDED, VCFHeaderLineType.String, "List of polymorphic samples")); }
}
