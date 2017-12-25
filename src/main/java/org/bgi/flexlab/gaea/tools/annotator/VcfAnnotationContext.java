/*******************************************************************************
 * Copyright (c) 2017, BGI-Shenzhen
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 *
 * This file incorporates work covered by the following copyright and 
 * Permission notices:
 *
 * Copyright (C)  2016  Pablo Cingolani(pcingola@users.sourceforge.net)
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/
package org.bgi.flexlab.gaea.tools.annotator;

import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import org.bgi.flexlab.gaea.tools.annotator.config.Config;
import org.bgi.flexlab.gaea.tools.annotator.interval.Chromosome;
import org.bgi.flexlab.gaea.tools.annotator.interval.Genome;
import org.bgi.flexlab.gaea.tools.annotator.interval.Variant;
import org.bgi.flexlab.gaea.tools.annotator.realignment.VcfRefAltAlign;

import java.util.*;

/**
 *  AnnotationContext + VariantContext
 */
public class VcfAnnotationContext extends VariantContext{

	private static final long serialVersionUID = -3258168300489497081L;
	public enum AlleleFrequencyType {
		Common, LowFrequency, Rare
	}
	public static final String FILTER_PASS = "PASS";
	
	private List<String> alts;
	
	protected LinkedList<Variant> variants;
	
	private List<AnnotationContext> annotationContexts;

	private boolean hasNearVar = false;
	
	public VcfAnnotationContext(VariantContext variantContext){
		super(variantContext);
		setAlts();
		variants = new LinkedList<Variant>();
	}
	
	/**
	 * Create a list of variants from this variantContext
	 */
	public List<Variant> variants(Genome genome) {
		if (!variants.isEmpty()) return variants;
		
		String refStr = this.getReference().getBaseString();

		// Create one Variant for each ALT
		Chromosome chr = genome.getChromosome(this.getContig());

		// interval 使用 0-base 方式建立，应使用start - 1创建variant对象
		if (!this.isVariant()) {
			// Not a variant?
			Variant variant = new Variant(chr, (int)start - 1, refStr, null, "");
			variant.setGenotype(".");
			variants.add(variant);
		} else {
			// At least one variant
			for (String alt : alts) {
				Variant variant = createVariant(chr, (int)start - 1, refStr, alt, "");
				variants.add(variant);
			}
		}
		return variants;
	}
	
	/**
	 * Create a variant
	 */
	Variant createVariant(Chromosome chromo, int start, String reference, String alt, String id) {
		Variant variant = null;
		if (alt != null) alt = alt.toUpperCase();

		if (alt == null || alt.isEmpty() || alt.equals(reference)) {
			// Non-variant
			variant = Variant.create(chromo, start, reference, null, id);
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
				variant = Variant.create(chromo, start, reference, alt, id);
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
				variant = Variant.create(chromo, start + startDiff, newRef, newAlt, id);
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
				variant = Variant.create(chromo, start + startDiff, ref, ch, id);
				break;

			case INS:
				// Case: Insertion of A { tC ; tCA } tC is the reference allele
				// 20     2 .         TC      TCA    .   PASS  DP=100
				ch = align.getAlignment();
				ref = "";
				if (!ch.startsWith("+")) throw new RuntimeException("Insertion '" + ch + "' does not start with '+'. This should never happen!");
				variant = Variant.create(chromo, start + startDiff, ref, ch, id);
				break;

			case MIXED:
				// Case: Mixed variant (substitution)
				reference = reference.substring(startDiff);
				alt = alt.substring(startDiff);
				variant = Variant.create(chromo, start + startDiff, reference, alt, id);
				break;

			default:
				// Other change type?
				throw new RuntimeException("Unsupported VCF change type '" + align.getVariantType() + "'\n\tRef: " + reference + "'\n\tAlt: '" + alt + "'\n\tVcfEntry: " + this);
			}
		}

		//---
		// Add original 'ALT' field as genotype
		//---
		if (variant == null) return null;
		variant.setGenotype(alt);

		return variant;
	}
	
	private void setAlts() {
		alts = new ArrayList<>();
		for (Allele allele : this.getAlleles()) {
			if (allele.isNonReference()) {
				alts.add(allele.toString());
			}
		}
	}
	
	public List<String> getAlts() {
		return alts;
	}
	
	public Set<String> getGenes() {
		Set<String> genes = new HashSet<String>();
		for (AnnotationContext ac : annotationContexts) {
			if (!ac.getGeneName().equals("")) {
				genes.add(ac.getGeneName());
			}
		}
		return genes;
	}

	public String toVcfLine() {
		// TODO Auto-generated method stub
		return null;
	}
	public String toAnnotationString(Config config) {
		StringBuilder sb = new StringBuilder();
		for (AnnotationContext annoContext : annotationContexts) {
			sb.append(this.contig);
			sb.append("\t");
			sb.append(this.start);
			sb.append("\t");
			sb.append(this.getReference().getBaseString());
			sb.append("\t");
			sb.append(annoContext.getFieldByName("ALLELE"));
			sb.append("\t");
			
			List<String> dbNameList = config.getDbNameList();
			for (String dbName : dbNameList) {
				
				ArrayList<String> fields = config.getFieldsByDB(dbName);
				
				if (dbName.equalsIgnoreCase("GeneInfo")) {
					for (String field : fields) {
						sb.append("\t");
						String tag = annoContext.getFieldByName(field);
						if (tag.isEmpty()) {
							sb.append(".");
						}else {
							sb.append(annoContext.getFieldByName(field));    
						}
					}
				}else {
					for (String field : fields) {
						sb.append("\t");
						sb.append(annoContext.getAnnoItemAsString(field, "."));       
					}
				}
			}
			sb.append("\n");
		}
		return sb.toString();
	}
	
	public LinkedList<Variant> getVariants(){
		return variants;
	}

	public void setAnnotationContexts(List<AnnotationContext> annotationContexts) {
		this.annotationContexts = annotationContexts;
	}
	
	public List<AnnotationContext> getAnnotationContexts() {
		return annotationContexts;
	}
	
	public String getChromeNoChr(){
		if(getContig().startsWith("chr")){
			return getContig().substring(3);
		}
		return getContig();
	}
	public String getChrome(){
		if(!getContig().startsWith("chr")){
			return "chr"+getContig();
		}
		return getContig();
	}


	public boolean hasNearVar() {
		return hasNearVar;
	}

	public void setHasNearVar(boolean hasNearVar) {
		this.hasNearVar = hasNearVar;
	}

}

