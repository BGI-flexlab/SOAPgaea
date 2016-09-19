package org.bgi.flexlab.gaea.tools.annotator.interval.codonChange;

import org.bgi.flexlab.gaea.tools.annotator.effect.EffectType;
import org.bgi.flexlab.gaea.tools.annotator.effect.VariantEffects;
import org.bgi.flexlab.gaea.tools.annotator.effect.VariantEffect.EffectImpact;
import org.bgi.flexlab.gaea.tools.annotator.interval.Exon;
import org.bgi.flexlab.gaea.tools.annotator.interval.Marker;
import org.bgi.flexlab.gaea.tools.annotator.interval.Transcript;
import org.bgi.flexlab.gaea.tools.annotator.interval.Variant;

/**
 * Calculate codon changes produced by an inversion
 *
 * @author pcingola
 */
public class CodonChangeInv extends CodonChange {

	public CodonChangeInv(Variant variant, Transcript transcript, VariantEffects variantEffects) {
		super(variant, transcript, variantEffects);
	}

	@Override
	public void codonChange() {
		if (variant.includes(transcript)) {
			// Whole transcript inverted?
			effectNoCodon(transcript, EffectType.TRANSCRIPT_INVERSION);
		} else {
			// Part of the transcript is inverted

			// Does the inversion affect any exon?
			boolean intersectsExons = false;
			for (Exon ex : transcript) {
				if (variant.intersects(ex)) {
					intersectsExons = true;
					break;
				}
			}

			// Annotate
			if (intersectsExons) exons();
			else intron();
		}
	}

	/**
	 * One or more exons fully included (no partial overlap)
	 */
	void exons() {
		Marker cdsMarker = null;
		if (transcript.isProteinCoding()) cdsMarker = transcript.cdsMarker();

		for (Exon ex : transcript)
			if (variant.intersects(ex)) {
				EffectImpact impact = EffectImpact.LOW;

				// Is the variant affecting a coding part of the exon?
				// If so, then this is a HIGH impact effect.
				if (cdsMarker != null && variant.intersect(ex).intersects(cdsMarker)) impact = EffectImpact.HIGH;

				// Is the whole exon inverted or just part of it?
				EffectType effType = variant.includes(ex) ? EffectType.EXON_INVERSION : EffectType.EXON_INVERSION_PARTIAL;

				effectNoCodon(ex, effType, impact);
			}
	}

	/**
	 * Inversion does not intersect any exon
	 */
	void intron() {
		effectNoCodon(transcript, EffectType.INTRON);
	}

}
