package org.bgi.flexlab.gaea.tools.annotator.effect;

import htsjdk.variant.variantcontext.VariantContext;

import java.util.List;

import org.bgi.flexlab.gaea.tools.annotator.config.Config;
import org.bgi.flexlab.gaea.tools.annotator.effect.VariantEffect.EffectImpact;
import org.bgi.flexlab.gaea.tools.annotator.interval.Variant;

/**
 * Annotate a VCF entry
 *
 */
public class VcfAnnotator {
	
	Config config;
	SnpEffectPredictor snpEffectPredictor;
	VariantContext variantContext;
	
	public VcfAnnotator(Config config){
		this.config = config;
		snpEffectPredictor = config.getSnpEffectPredictor();
	}
	
	/**
	 * Annotate a VCF entry
	 *
	 * @return true if the entry was annotated
	 */
	public boolean annotate(VcfAnnotationContext vac) {
		boolean filteredOut = false;
		//---
		// Analyze all changes in this VCF entry
		// Note, this is the standard analysis.
		//---
		List<Variant> variants = vac.variants();
		for (Variant variant : variants) {

			// Calculate effects: By default do not annotate non-variant sites
			if (variant.isVariant()) {

				VariantEffects variantEffects = snpEffectPredictor.variantEffect(variant);

			}
		}

		return true;
	}

}
