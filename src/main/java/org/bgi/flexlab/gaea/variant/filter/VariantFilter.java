package org.bgi.flexlab.gaea.variant.filter;

import java.io.IOException;
import java.util.ArrayList;

import org.bgi.flexlab.gaea.data.structure.vcf.index.VCFLoader;

import htsjdk.variant.variantcontext.VariantContext;

public abstract class VariantFilter {
	public abstract int filter(VariantContext context, int start, int end);

	public ArrayList<VariantContext> listFilter(ArrayList<VariantContext> contexts, int start, int end) {
		ArrayList<VariantContext> filter = new ArrayList<VariantContext>();

		if (contexts != null) {
			for (VariantContext context : contexts) {
				int result = filter(context, start, end);
				if (result == 0)
					continue;
				if (result == 1)
					filter.add(context);
				else
					break;
			}
		}
		return filter;
	}

	public ArrayList<VariantContext> loadFilter(VCFLoader loader, String referenceName, int start, int end) {
		try {
			return loader.load(referenceName, start, end);
		} catch (IOException e) {
			throw new RuntimeException(e.toString());
		}
	}
}
