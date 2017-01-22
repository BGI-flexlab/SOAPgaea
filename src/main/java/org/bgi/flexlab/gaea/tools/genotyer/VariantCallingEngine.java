package org.bgi.flexlab.gaea.tools.genotyer;

import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import org.bgi.flexlab.gaea.data.structure.pileup2.Mpileup;
import org.bgi.flexlab.gaea.data.structure.pileup2.Pileup;
import org.bgi.flexlab.gaea.data.structure.pileup2.ReadsPool;
import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformationShare;
import org.bgi.flexlab.gaea.tools.genotyer.GenotypeLikelihoodCalculator.GenotypeData;
import org.bgi.flexlab.gaea.tools.genotyer.GenotypeLikelihoodCalculator.GenotypeLikelihoodCalculator;
import org.bgi.flexlab.gaea.tools.genotyer.genotypecaller.GenotyperCaller;
import org.bgi.flexlab.gaea.tools.mapreduce.genotyper.GenotyperOptions;
import org.bgi.flexlab.gaea.util.BaseUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by zhangyong on 2016/12/20.
 */
public class VariantCallingEngine {

    public enum OUTPUT_MODE {
        /** produces calls only at variant sites */
        EMIT_VARIANTS_ONLY,
        /** produces calls at variant sites and confident reference sites */
        EMIT_ALL_CONFIDENT_SITES,
        /** produces calls at any callable site regardless of confidence; this argument is intended only for point
         * mutations (SNPs) in DISCOVERY mode or generally when running in GENOTYPE_GIVEN_ALLELES mode; it will by
         * no means produce a comprehensive set of indels in DISCOVERY mode */
        EMIT_ALL_SITES
    }

    /**
     * Mpileup struct
     */
    private Mpileup mpileup;

    /**
     * memory shared reference
     */
    private ChromosomeInformationShare reference;

    /**
     * genotype caller
     */
    private GenotyperCaller genotyperCaller;

    /**
     * options
     */
    private GenotyperOptions options;

    /**
     * constructor
     * @param readsPool reads
     * @param windowStart window start
     * @param windowEnd window end
     */
    public VariantCallingEngine(ReadsPool readsPool, ChromosomeInformationShare reference, int windowStart, int windowEnd, GenotyperOptions options) {
        mpileup = new Mpileup(readsPool, windowStart, windowEnd);
        this.reference = reference;
        this.options = options;
    }

    public List<VariantContext> reduce() {
        List<VariantContext> vcList = new ArrayList<>();
        for(GenotypeLikelihoodCalculator.Model model : GenotypeLikelihoodCalculator.modelsToUse) {
            VariantContext vc = GenotypeLikelihoodCalculator.glcm.get(model).genotypeLikelihoodCalculate(mpileup, reference, options);
            vcList.add(vc);
        }

        return vcList;
    }
}
