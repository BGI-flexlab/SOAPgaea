package org.bgi.flexlab.gaea.tools.realigner;

import htsjdk.variant.variantcontext.VariantContext;

import java.util.Comparator;

@SuppressWarnings("rawtypes")
public class KnowIndelComparator implements Comparator{

	@Override
	public int compare(Object obj1, Object obj2) {
		VariantContext variant1 = (VariantContext) obj1;
		VariantContext variant2 = (VariantContext) obj2;
		
		if(variant1.getContig() != variant2.getContig())
			return variant1.getContig().compareTo(variant2.getContig());
		
		if(variant1.getStart() != variant2.getStart())
			return variant1.getStart() - variant2.getStart();
		
		return variant1.getEnd()-variant2.getEnd();
	}
}
