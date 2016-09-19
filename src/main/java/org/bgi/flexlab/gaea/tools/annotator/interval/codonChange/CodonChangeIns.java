package org.bgi.flexlab.gaea.tools.annotator.interval.codonChange;

import org.bgi.flexlab.gaea.tools.annotator.effect.EffectType;
import org.bgi.flexlab.gaea.tools.annotator.effect.VariantEffects;
import org.bgi.flexlab.gaea.tools.annotator.interval.Exon;
import org.bgi.flexlab.gaea.tools.annotator.interval.Transcript;
import org.bgi.flexlab.gaea.tools.annotator.interval.Variant;

/**
 * Calculate codon changes produced by an insertion
 * @author pcingola
 */
public class CodonChangeIns extends CodonChange {

	public CodonChangeIns(Variant seqChange, Transcript transcript, VariantEffects changeEffects) {
		super(seqChange, transcript, changeEffects);
		returnNow = true; // An insertion can only affect one exon
	}

	/**
	 * Analyze insertions in this transcript.
	 * Add changeEffect to 'changeEffect'
	 */
	@Override
	protected boolean codonChange(Exon exon) {
		String netChange = variant.netChange(transcript.isStrandMinus());

		codonsRef = codonsRef();
		codonsAlt = codonsAlt();

		EffectType effType = null;

		if (netChange.length() % CodonChange.CODON_SIZE != 0) {
			/**
			 * Length not multiple of CODON_SIZE => FRAME_SHIFT
			 * 	E.g. :
			 * 		Original:			AAA CCC GGG AAA CCC GGG AAA CCC GGG
			 * 		Insert 'TT' pos 0:	TTA AAC CCG GGA AAC CCG GGA AAC CCG GG
			 * 		Insert 'TT' pos 1:	ATT AAC CCG GGA AAC CCG GGA AAC CCG GG
			 * 		Insert 'TT' pos 2:	AAT TAC CCG GGA AAC CCG GGA AAC CCG GG
			 */
			effType = EffectType.FRAME_SHIFT;
		} else if (codonStartIndex == 0) {
			/**
			 * Length multiple of CODON_SIZE and insertion happens at codon boundary => CODON_INSERTION
			 * 	E.g. :
			 * 		Original:			AAA CCC GGG AAA CCC GGG AAA CCC GGG
			 * 		Insert 'TTT' pos 0:	TTT AAA CCC GGG AAA CCC GGG AAA CCC GGG
			 */
			effType = EffectType.CODON_INSERTION;
		} else {
			/**
			 * Length multiple of CODON_SIZE and insertion does not happen at codon boundary => CODON_CHANGE_PLUS_CODON_INSERTION
			 * 	E.g. :
			 * 		Original:			AAA CCC GGG AAA CCC GGG AAA CCC GGG
			 * 		Insert 'TTT' pos 1:	ATT TAA CCC GGG AAA CCC GGG AAA CCC GGG
			 * 		Insert 'TTT' pos 2:	AAT TTA CCC GGG AAA CCC GGG AAA CCC GGG
			 */
			if (codonsAlt.toUpperCase().startsWith(codonsRef.toUpperCase())) {
				/**
				 *  May be the inserted base are equal to the old ones.
				 *  E.g.
				 *  	Original:			AAA CCC GGG AAA CCC GGG AAA CCC GGG
				 *  	Insert 'AAA' pos 1:	AAA AAA CCC GGG AAA CCC GGG AAA CCC GGG
				 */
				effType = EffectType.CODON_INSERTION;
			} else {
				effType = EffectType.CODON_CHANGE_PLUS_CODON_INSERTION;
			}
		}

		effect(exon, effType, false);

		return true;
	}

	/**
	 * Get new (modified) codons
	 */
	@Override
	protected String codonsAlt() {
		// Inserts BEFORE base:
		//		- In positive strand that is BEFORE pos
		//		- In negative strand, that is AFTER pos
		int idx = codonStartIndex + (transcript.isStrandMinus() ? 1 : 0);

		// Insertion: Concatenate...
		String prefix = codonsRef.length() >= idx ? codonsRef.substring(0, idx) : codonsRef; // First part of the codon
		String netChange = variant.netChange(transcript.isStrandMinus()); // Insertion
		String suffix = codonsRef.length() >= idx ? codonsRef.substring(idx) : ""; // last part of the codon

		// New codon
		String codonsNew = prefix + netChange + suffix;

		return codonsNew;
	}

}
