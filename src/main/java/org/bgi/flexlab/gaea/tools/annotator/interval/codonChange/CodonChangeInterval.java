package org.bgi.flexlab.gaea.tools.annotator.interval.codonChange;

import org.bgi.flexlab.gaea.tools.annotator.effect.VariantEffects;
import org.bgi.flexlab.gaea.tools.annotator.interval.Exon;
import org.bgi.flexlab.gaea.tools.annotator.interval.Transcript;
import org.bgi.flexlab.gaea.tools.annotator.interval.Variant;

/**
 * Calculate codon changes produced by a Interval
 *
 * Note: An interval does not produce any effect.
 *
 * @author pcingola
 */
public class CodonChangeInterval extends CodonChange {

	public CodonChangeInterval(Variant seqChange, Transcript transcript, VariantEffects changeEffects) {
		super(seqChange, transcript, changeEffects);
		returnNow = false; // An interval may affect more than one exon
	}

	/**
	 * Interval is not a variant, nothing to do
	 */
	@Override
	protected boolean codonChange(Exon exon) {
		return false;
	}

}
