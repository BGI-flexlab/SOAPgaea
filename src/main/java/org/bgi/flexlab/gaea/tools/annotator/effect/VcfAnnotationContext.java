package org.bgi.flexlab.gaea.tools.annotator.effect;

import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import org.bgi.flexlab.gaea.tools.annotator.interval.Chromosome;
import org.bgi.flexlab.gaea.tools.annotator.interval.Variant;

/**
 *  AnnotationContext + VariantContext
 */
public class VcfAnnotationContext implements Serializable{
	
	private static final long serialVersionUID = -5800632419137710193L;
	
	public enum AlleleFrequencyType {
		Common, LowFrequency, Rare
	}
	public static final String FILTER_PASS = "PASS";
	
	protected VariantContext variantContext;
	protected LinkedList<Variant> variants;
	
	protected List<EffectContext> effectContexts;
	
	public VcfAnnotationContext(){
		
	}

	public VcfAnnotationContext(VariantContext variantContext){
		this.variantContext = variantContext;
	}
	
	
	/**
	 * Create a list of variants from this variantContext
	 */
	public List<Variant> variants() {
		if (variants != null) return variants;

		// Create list of variants
		variants = new LinkedList<Variant>();

		// Create one Variant for each ALT
		Chromosome chr = (Chromosome) parent;

		if (!variantContext.isVariant()) {
			// Not a variant?
			List<Variant> vars = variants(chr, start, ref, null, id);
			String alt = ".";

			// Add original 'ALT' field as genotype
			for (Variant variant : vars)
				variant.setGenotype(alt);

			variants.addAll(vars);
		} else {
			// At least one variant
			for (Allele alt : variantContext.getAlleles()) {
				if (!isVariant(alt)) alt = null;
				List<Variant> vars = variants(chr, start, ref, alt, id);
				variants.addAll(vars);
			}
		}
	
		return variants;
	}

	public String toVcfLine() {
		// TODO Auto-generated method stub
		return null;
	}

	

}


