package org.bgi.flexlab.gaea.tools.jointcalling;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.bgi.flexlab.gaea.data.structure.location.GenomeLocation;
import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformationShare;
import org.bgi.flexlab.gaea.tools.jointcalling.util.GaeaGvcfVariantContextUtils;
import org.bgi.flexlab.gaea.tools.jointcalling.util.RefMetaDataTracker;
import org.bgi.flexlab.gaea.util.GaeaVCFConstants;

import htsjdk.samtools.SAMRecord;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypeBuilder;
import htsjdk.variant.variantcontext.GenotypesContext;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;
import htsjdk.variant.vcf.VCFConstants;

public class JointCallingEngine {
	private int start;
	private int end;
	private String contig;
	
	public boolean INCLUDE_NON_VARIANTS = false;
	
	private ArrayList<VariantContext> variants = null;
	
	// the genotyping engine
    private UnifiedGenotypingEngine genotypingEngine;
    // the annotation engine
    private VariantAnnotatorEngine annotationEngine;
	
	public JointCallingEngine(){
		this.contig = SAMRecord.NO_ALIGNMENT_REFERENCE_NAME;
		this.start = 0;
		this.end = 0;
		
		variants = new ArrayList<VariantContext>();
	}
	
	public void set(String contig,int start,int end){
		this.contig = contig;
		this.start = start;
		this.end = end;
	}
	
	public VariantContext variantCalling(Iterator iterator,GenomeLocation location){
		VariantContext result = null;
		
		return result;
	}
	
	protected VariantContext regenotypeVC(final RefMetaDataTracker tracker, final ChromosomeInformationShare ref, final VariantContext originalVC) {
        if ( originalVC == null ) {
            throw new IllegalArgumentException("originalVC cannot be null");
        } else if (!isProperlyPolymorphic(originalVC) && !INCLUDE_NON_VARIANTS) {
            return null;
        }

        VariantContext result = originalVC;

        //don't need to calculate quals for sites with no data whatsoever
        if (result.getAttributeAsInt(VCFConstants.DEPTH_KEY,0) > 0 ) {
            result = genotypingEngine.calculateGenotypes(originalVC);
        } 

        if (result == null || (!isProperlyPolymorphic(result) && !INCLUDE_NON_VARIANTS)) {
            return null;
        }

        result = addGenotypingAnnotations(originalVC.getAttributes(), result);
        //At this point we should already have DP and AD annotated
        result = annotationEngine.finalizeAnnotations(result, originalVC);
        //do trimming after allele-specific annotation reduction or the mapping is difficult
        result = GaeaGvcfVariantContextUtils.reverseTrimAlleles(result);


        // Re-annotate and fix/remove some of the original annotations.
        // Note that the order of these actions matters and is different for polymorphic and monomorphic sites.
        // For polymorphic sites we need to make sure e.g. the SB tag is sent to the annotation engine and then removed later.
        // For monomorphic sites we need to make sure e.g. the hom ref genotypes are created and only then are passed to the annotation engine.
        // We could theoretically make 2 passes to re-create the genotypes, but that gets extremely expensive with large sample sizes.
        if (result.isPolymorphicInSamples()) {
            result = annotationEngine.annotateContext(tracker, ref, result);
            result = new VariantContextBuilder(result).genotypes(cleanupGenotypeAnnotations(result, false)).make();
        } else if (INCLUDE_NON_VARIANTS) {
            result = new VariantContextBuilder(result).genotypes(cleanupGenotypeAnnotations(result, true)).make();
            result = annotationEngine.annotateContext(tracker, ref, result);
        } else {
            return null;
        }
        return result;
    }
	
	private boolean isProperlyPolymorphic(final VariantContext vc) {
	    //obvious cases
	    if (vc == null || vc.getAlternateAlleles().isEmpty()) {
	        return false;
	    } else if (vc.isBiallelic()) {
	        return !(vc.getAlternateAllele(0).equals(Allele.SPAN_DEL) ||
	                vc.getAlternateAllele(0).equals(GaeaVCFConstants.SPANNING_DELETION_SYMBOLIC_ALLELE_DEPRECATED) ||
	                vc.isSymbolic());
	    } else {
	        return true;
	    }
	}
	
	private VariantContext addGenotypingAnnotations(final Map<String, Object> originalAttributes, final VariantContext newVC) {
	    // we want to carry forward the attributes from the original VC but make sure to add the MLE-based annotations
	    final Map<String, Object> attrs = new HashMap<>(originalAttributes);
	    attrs.put(GaeaVCFConstants.MLE_ALLELE_COUNT_KEY, newVC.getAttribute(GaeaVCFConstants.MLE_ALLELE_COUNT_KEY));
	    attrs.put(GaeaVCFConstants.MLE_ALLELE_FREQUENCY_KEY, newVC.getAttribute(GaeaVCFConstants.MLE_ALLELE_FREQUENCY_KEY));
	    if (newVC.hasAttribute(GaeaVCFConstants.NUMBER_OF_DISCOVERED_ALLELES_KEY))
	        attrs.put(GaeaVCFConstants.NUMBER_OF_DISCOVERED_ALLELES_KEY, newVC.getAttribute(GaeaVCFConstants.NUMBER_OF_DISCOVERED_ALLELES_KEY));
	    if (newVC.hasAttribute(GaeaVCFConstants.AS_QUAL_KEY))
	        attrs.put(GaeaVCFConstants.AS_QUAL_KEY, newVC.getAttribute(GaeaVCFConstants.AS_QUAL_KEY));

	    return new VariantContextBuilder(newVC).attributes(attrs).make();
	}
	
	private List<Genotype> cleanupGenotypeAnnotations(final VariantContext VC, final boolean createRefGTs) {
	    final GenotypesContext oldGTs = VC.getGenotypes();
	    final List<Genotype> recoveredGs = new ArrayList<>(oldGTs.size());
	    for ( final Genotype oldGT : oldGTs ) {
	        final Map<String, Object> attrs = new HashMap<>(oldGT.getExtendedAttributes());

	        final GenotypeBuilder builder = new GenotypeBuilder(oldGT);
	        int depth = oldGT.hasDP() ? oldGT.getDP() : 0;

	        // move the MIN_DP to DP
	        if ( oldGT.hasExtendedAttribute(GaeaVCFConstants.MIN_DP_FORMAT_KEY) ) {
	            depth = Integer.parseInt((String)oldGT.getAnyAttribute(GaeaVCFConstants.MIN_DP_FORMAT_KEY));
	            builder.DP(depth);
	            attrs.remove(GaeaVCFConstants.MIN_DP_FORMAT_KEY);
	        }

	        // move the GQ to RGQ
	        if ( createRefGTs && oldGT.hasGQ() ) {
	            builder.noGQ();
	            attrs.put(GaeaVCFConstants.REFERENCE_GENOTYPE_QUALITY, oldGT.getGQ());
	        }

	        // remove SB
	        attrs.remove(GaeaVCFConstants.STRAND_BIAS_BY_SAMPLE_KEY);

	        // update PGT for hom vars
	        if ( oldGT.isHomVar() && oldGT.hasExtendedAttribute(GaeaVCFConstants.HAPLOTYPE_CALLER_PHASING_GT_KEY) ) {
	            attrs.put(GaeaVCFConstants.HAPLOTYPE_CALLER_PHASING_GT_KEY, "1|1");
	        }

	        // create AD if it's not there
	        if ( !oldGT.hasAD() && VC.isVariant() ) {
	            final int[] AD = new int[VC.getNAlleles()];
	            AD[0] = depth;
	            builder.AD(AD);
	        }

	        if ( createRefGTs ) {
	            final int ploidy = oldGT.getPloidy();
	            final List<Allele> refAlleles = Collections.nCopies(ploidy,VC.getReference());

	            //keep 0 depth samples and 0 GQ samples as no-call
	            if (depth > 0 && oldGT.hasGQ() && oldGT.getGQ() > 0) {
	                builder.alleles(refAlleles);
	            }

	            // also, the PLs are technically no longer usable
	            builder.noPL();
	        }

	        recoveredGs.add(builder.noAttributes().attributes(attrs).make());
	    }
	    return recoveredGs;
	}
}
