package org.bgi.flexlab.gaea.variant.filter;

import htsjdk.variant.variantcontext.VariantContext;

public class VariantRegionFilter extends VariantFilter {

	@Override
	public int filter(VariantContext context, int start, int end) {
		if ((context.getStart() >= start && context.getStart() <= end)
				|| (context.getEnd() >= start && context.getEnd() <= end)) {
			return 1;
		} else if (context.getStart() > end) {
			return -1;
		}
		return 0;
	}
}
