package org.bgi.flexlab.gaea.tools.annotator.effect;

import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.bgi.flexlab.gaea.tools.annotator.interval.Chromosome;
import org.bgi.flexlab.gaea.tools.annotator.interval.Genome;
import org.bgi.flexlab.gaea.tools.annotator.interval.Variant;
import org.bgi.flexlab.gaea.tools.annotator.realignment.VcfRefAltAlign;

/**
 *  AnnotationContext + VariantContext
 */
public class VcfAnnotationContext extends VariantContext{
	
	public enum AlleleFrequencyType {
		Common, LowFrequency, Rare
	}
	public static final String FILTER_PASS = "PASS";
	
	List<String> alts;
	
	protected LinkedList<Variant> variants;
	
	HashMap<String, AnnotationContext> annotationContexts;
	
	public VcfAnnotationContext(VariantContext variantContext){
		super(variantContext);
		setAlts();
	}

	
	/**
	 * Create a list of variants from this variantContext
	 */
	public List<Variant> variants(Genome genome) {
		if (variants != null) return variants;

		// Create list of variants
		variants = new LinkedList<Variant>();

		// Create one Variant for each ALT
//		Chromosome chr = (Chromosome) parent;
		Chromosome chr = genome.getChromosome(this.getContig());

		if (!this.isVariant()) {
			// Not a variant?
			List<Variant> vars = variants(chr, (int)start, this.getReference().toString(), null, "");
			String alt = ".";

			// Add original 'ALT' field as genotype
			for (Variant variant : vars)
				variant.setGenotype(alt);

			variants.addAll(vars);
		} else {
			// At least one variant
			String altStr = listToString(alts, ",");
			List<Variant> vars = variants(chr, (int)start, this.getReference().toString(), altStr, "");
			variants.addAll(vars);
		}
	
		return variants;
	}
	
	/**
	 * Create a variant
	 */
	List<Variant> variants(Chromosome chromo, int start, String reference, String alt, String id) {
		List<Variant> list = null;
		if (alt != null) alt = alt.toUpperCase();

		if (alt == null || alt.isEmpty() || alt.equals(reference)) {
			// Non-variant
			list = Variant.factory(chromo, start, reference, null, id, false);
		} else if (alt.charAt(0) == '<') {
			// TODO Structural variants 
			System.err.println("Cann't annotate Structural variants! ");
		} else if ((alt.indexOf('[') >= 0) || (alt.indexOf(']') >= 0)) {
			// TODO Translocations
			System.err.println("Cann't annotate Translocations: ALT has \"[\" or \"]\" info!");
			
		} else if (reference.length() == alt.length()) {
			// Case: SNP, MNP
			if (reference.length() == 1) {
				// SNPs
				// 20     3 .         C      G       .   PASS  DP=100
				list = Variant.factory(chromo, start, reference, alt, id, true);
			} else {
				// MNPs
				// 20     3 .         TC     AT      .   PASS  DP=100
				// Sometimes the first bases are the same and we can trim them
				int startDiff = Integer.MAX_VALUE;
				for (int i = 0; i < reference.length(); i++)
					if (reference.charAt(i) != alt.charAt(i)) startDiff = Math.min(startDiff, i);

				// MNPs
				// Sometimes the last bases are the same and we can trim them
				int endDiff = 0;
				for (int i = reference.length() - 1; i >= 0; i--)
					if (reference.charAt(i) != alt.charAt(i)) endDiff = Math.max(endDiff, i);

				String newRef = reference.substring(startDiff, endDiff + 1);
				String newAlt = alt.substring(startDiff, endDiff + 1);
				list = Variant.factory(chromo, start + startDiff, newRef, newAlt, id, true);
			}
		} else {
			// Short Insertions, Deletions or Mixed Variants (substitutions)
			VcfRefAltAlign align = new VcfRefAltAlign(alt, reference);
			align.align();
			int startDiff = align.getOffset();

			switch (align.getVariantType()) {
			case DEL:
				// Case: Deletion
				// 20     2 .         TC      T      .   PASS  DP=100
				// 20     2 .         AGAC    AAC    .   PASS  DP=100
				String ref = "";
				String ch = align.getAlignment();
				if (!ch.startsWith("-")) throw new RuntimeException("Deletion '" + ch + "' does not start with '-'. This should never happen!");
				list = Variant.factory(chromo, start + startDiff, ref, ch, id, true);
				break;

			case INS:
				// Case: Insertion of A { tC ; tCA } tC is the reference allele
				// 20     2 .         TC      TCA    .   PASS  DP=100
				ch = align.getAlignment();
				ref = "";
				if (!ch.startsWith("+")) throw new RuntimeException("Insertion '" + ch + "' does not start with '+'. This should never happen!");
				list = Variant.factory(chromo, start + startDiff, ref, ch, id, true);
				break;

			case MIXED:
				// Case: Mixed variant (substitution)
				reference = reference.substring(startDiff);
				alt = alt.substring(startDiff);
				list = Variant.factory(chromo, start + startDiff, reference, alt, id, true);
				break;

			default:
				// Other change type?
				throw new RuntimeException("Unsupported VCF change type '" + align.getVariantType() + "'\n\tRef: " + reference + "'\n\tAlt: '" + alt + "'\n\tVcfEntry: " + this);
			}
		}

		//---
		// Add original 'ALT' field as genotype
		//---
		if (list == null) list = new LinkedList<>();
		for (Variant variant : list)
			variant.setGenotype(alt);

		return list;
	}
	
	
	private void setAlts() {
		alts = new ArrayList<String>();
		for (Allele allele : this.getAlleles()) {
			if (allele.isNonReference()) {
				alts.add(allele.toString());
			}
		}
	}

	public String toVcfLine() {
		// TODO Auto-generated method stub
		return null;
	}
	
	private String listToString(List<String> list, String separator) { 
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < list.size()-1; i++) { 
			sb.append(list.get(i));       
			sb.append(separator);
		}
		sb.append(list.get(list.size()-1));
		return sb.toString();
	}

	

}


