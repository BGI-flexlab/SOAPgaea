package org.bgi.flexlab.gaea.tools.genotyer.genotypeLikelihoodCalculator;

import org.bgi.flexlab.gaea.tools.mapreduce.genotyper.GenotyperOptions;

/**
 * Created by zhangyong on 2016/12/21.
 */
public class SOAPSNPGenotypeLikelihoodCalculator extends SNPGenotypeLikelihoodCalculator {
    public SOAPSNPGenotypeLikelihoodCalculator(GenotyperOptions options, double pcrErrorRate) {
        super(options, pcrErrorRate);
    }
}
