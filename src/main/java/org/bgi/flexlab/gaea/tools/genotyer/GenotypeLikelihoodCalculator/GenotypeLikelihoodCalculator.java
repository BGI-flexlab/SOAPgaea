package org.bgi.flexlab.gaea.tools.genotyer.genotypelikelihoodcalculator;

import htsjdk.variant.variantcontext.GenotypesContext;
import org.bgi.flexlab.gaea.data.structure.pileup.Pileup;

/**
 * Created by zhangyong on 2016/12/21.
 */
public abstract class GenotypeLikelihoodCalculator {
    public abstract GenotypesContext genotypeLikelihoodCalculate(Pileup pileup);
}
