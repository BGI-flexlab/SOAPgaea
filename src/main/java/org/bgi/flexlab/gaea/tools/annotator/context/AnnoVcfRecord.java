package org.bgi.flexlab.gaea.tools.annotator.context;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.GenotypesContext;
import htsjdk.variant.variantcontext.VariantContext;

public class AnnoVcfRecord extends VariantContext {
	
	public enum AlleleFrequencyType {
		Common, LowFrequency, Rare
	}
	public static final String FILTER_PASS = "PASS";
	
	AnnoContexts annoContexts = null;

	protected AnnoVcfRecord(String source, String ID, String contig,
			long start, long stop, Collection<Allele> alleles,
			GenotypesContext genotypes, double log10pError,
			Set<String> filters, Map<String, Object> attributes,
			boolean fullyDecoded, EnumSet<Validation> validationToPerform) {
		
		super(source, ID, contig, start, stop, alleles, genotypes, log10pError,
				filters, attributes, fullyDecoded, validationToPerform);
		annoContexts = new AnnoContexts();
	}

	
	private static final long serialVersionUID = -5800632419137710193L;

}


