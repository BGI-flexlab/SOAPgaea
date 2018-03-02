package org.bgi.flexlab.gaea.data.mapreduce.output.vcf;

import htsjdk.variant.variantcontext.VariantContext;

public interface GaeaVariantContextWriter {
	public void write(VariantContext context);
}
