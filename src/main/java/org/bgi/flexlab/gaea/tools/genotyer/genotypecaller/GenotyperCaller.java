package org.bgi.flexlab.gaea.tools.genotyer.genotypecaller;

import htsjdk.variant.variantcontext.VariantContext;

/**
 * Created by zhangyong on 2016/12/21.
 */
public abstract class GenotyperCaller {
    abstract VariantContext genotyperCalling();
}
