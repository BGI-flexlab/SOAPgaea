package org.bgi.flexlab.gaea.tools.annotator.interval;

import java.util.ArrayList;

import org.bgi.flexlab.gaea.tools.annotator.conf.Config;
import org.bgi.flexlab.gaea.tools.annotator.effect.EffectType;
import org.bgi.flexlab.gaea.tools.annotator.effect.VariantEffects;
import org.bgi.flexlab.gaea.tools.annotator.effect.VariantEffect.ErrorWarningType;
import org.bgi.flexlab.gaea.tools.annotator.interval.Variant.VariantType;
import org.bgi.flexlab.gaea.tools.annotator.interval.codonChange.CodonChange;
import org.bgi.flexlab.gaea.tools.annotator.util.Gpr;

/**
 * Interval for an exon
 *
 * @author pcingola
 */
public class Exon extends MarkerSeq implements MarkerWithFrame {

	/**
	 * Characterize exons based on alternative splicing
	 * References: "Alternative splicing and evolution - diversification, exon definition and function"  (see Box 1)
	 */
	public enum ExonSpliceType {
		NONE, // Not spliced
		RETAINED, // All transcripts have this exon
		SKIPPED, // Some transcripts skip it
		ALTTENATIVE_3SS, // Some transcripts have and alternative 3' exon start
		ALTTENATIVE_5SS, // Some transcripts have and alternative 5' exon end
		MUTUALLY_EXCLUSIVE, // Mutually exclusive (respect to other exon)
		ALTTENATIVE_PROMOMOTER, // The first exon is different in some transcripts.
		ALTTENATIVE_POLY_A, // The last exon.
	}

	public static int ToStringVersion = 2;

	private static final long serialVersionUID = 5324352193278472543L;

	byte frame = -1; // Phase can be {-1, 0, 1, 2}, where '-1' means unknown. Phase indicated the number of bases that should be removed from the beginning of this feature to reach the first base of the next codon
	int rank; // Exon rank in transcript
	int aaIdxStart = -1, aaIdxEnd = -1; // First and last AA indexes that intersect with this exon
	ArrayList<SpliceSite> spliceSites;
	ExonSpliceType spliceType = ExonSpliceType.NONE;

	public Exon() {
		super();
		rank = 0;
		type = EffectType.EXON;
		spliceSites = new ArrayList<SpliceSite>();
	}

	public Exon(Transcript parent, int start, int end, boolean strandMinus, String id, int rank) {
		super(parent, start, end, strandMinus, id);
		this.strandMinus = strandMinus;
		this.rank = rank;
		type = EffectType.EXON;
		spliceSites = new ArrayList<SpliceSite>();
	}

	/**
	 * Add a splice site to the collection
	 */
	public void add(SpliceSite ss) {
		spliceSites.add(ss);
	}

	/**
	 * Apply variant to exon
	 *
	 * WARNING: There might be conditions which change the exon
	 *          type (e.g. an intron is deleted). Nevertheless ExonSpliceType
	 *          is not updated since it reflects the exon type
	 *          before a sequence change.
	 */
	@Override
	public Exon apply(Variant variant) {
		// Create new exon with updated coordinates
		Exon newEx = (Exon) super.apply(variant);
		if (newEx == null) return null;

		// Splice sites should be created using Transcript.createSpliceSites() method
		newEx.reset();

		return newEx;
	}

	@Override
	public Exon cloneShallow() {
		Exon clone = (Exon) super.cloneShallow();
		clone.frame = frame;
		clone.rank = rank;
		clone.spliceType = spliceType;
		return clone;
	}

	/**
	 * Create splice site regions
	 */
	public SpliceSiteRegion createSpliceSiteRegionEnd(int size) {
		if (size > size()) size = size(); // Cannot be larger than this marker
		if (size <= 0) return null;

		SpliceSiteRegion spliceSiteRegionEnd = null;
		if (isStrandPlus()) spliceSiteRegionEnd = new SpliceSiteRegion(this, end - (size - 1), end, strandMinus, id);
		else spliceSiteRegionEnd = new SpliceSiteRegion(this, start, start + (size - 1), strandMinus, id);

		if (spliceSiteRegionEnd != null) add(spliceSiteRegionEnd);

		return spliceSiteRegionEnd;
	}

	/**
	 * Create splice site regions
	 */
	public SpliceSiteRegion createSpliceSiteRegionStart(int size) {
		if (size > size()) size = size(); // Cannot be larger than this marker
		if (size <= 0) return null;

		SpliceSiteRegion spliceSiteRegionStart = null;
		if (isStrandPlus()) spliceSiteRegionStart = new SpliceSiteRegion(this, start, start + (size - 1), strandMinus, id);
		else spliceSiteRegionStart = new SpliceSiteRegion(this, end - (size - 1), end, strandMinus, id);

		if (spliceSiteRegionStart != null) add(spliceSiteRegionStart);

		return spliceSiteRegionStart;
	}

	/**
	 * Correct exons according to frame information
	 * Shift the start position one base
	 */
	public boolean frameCorrection(int frameCorrection) {
		if (frameCorrection <= 0) return true; // Nothing to do

		// Can correct?
		if (size() <= frameCorrection) {
			Gpr.debug("Exon too short (size: " + size() + "), cannot correct frame!\n" + this);
			return false;
		}

		// Correct start or end coordinates
		if (isStrandPlus()) start += frameCorrection;
		else end -= frameCorrection;

		// Correct frame
		frame = (byte) ((frame - frameCorrection) % 3);
		while (frame < 0)
			frame += 3;

		// Correct sequence
		String sequence = getSequence();
		if (sequence.length() >= frameCorrection) sequence = sequence.substring(frameCorrection);
		setSequence(sequence);

		return true;
	}

	public int getAaIdxEnd() {
		return aaIdxEnd;
	}

	public int getAaIdxStart() {
		return aaIdxStart;
	}

	@Override
	public int getFrame() {
		return frame;
	}

	public int getRank() {
		return rank;
	}

	public ArrayList<SpliceSite> getSpliceSites() {
		return spliceSites;
	}

	public ExonSpliceType getSpliceType() {
		return spliceType;
	}

	@Override
	protected boolean isAdjustIfParentDoesNotInclude(Marker parent) {
		return true;
	}

	/**
	 * Query all genomic regions that intersect 'marker'
	 */
	@Override
	public Markers query(Marker marker) {
		Markers markers = new Markers();

		for (SpliceSite ss : spliceSites)
			if (ss.intersects(marker)) markers.add(ss);

		return markers;
	}

	public void reset() {
		spliceSites = new ArrayList<SpliceSite>();
	}

	/**
	 * Check that the base in the exon corresponds with the one in the SNP
	 */
	public ErrorWarningType sanityCheck(Variant variant) {
		if (!intersects(variant)) return null;

		// Only makes sense for SNPs and MNPs
		if ((variant.getVariantType() != VariantType.SNP) && (variant.getVariantType() != VariantType.MNP)) return null;

		// Calculate reference sequence coordinates
		int mstart = Math.max(variant.getStart(), start);
		int idxStart = mstart - start;

		if (sequence.length() <= 0) return ErrorWarningType.WARNING_SEQUENCE_NOT_AVAILABLE;
		if (idxStart >= sequence.length()) return ErrorWarningType.ERROR_OUT_OF_EXON;

		int mend = Math.min(variant.getEnd(), end);
		int len = mend - mstart + 1;

		String realReference = basesAt(idxStart, len).toUpperCase();

		// Get variant's reference coordinates
		int varRefStart = mstart - variant.getStart();
		if (varRefStart < 0) return ErrorWarningType.ERROR_OUT_OF_EXON;

		int varRefEnd = mend - variant.getStart();

		// Variant's reference sequence
		String refStr;
		if (variant.isNonRef()) refStr = ((VariantNonRef) variant).getVariantRef().getAlt();
		else refStr = variant.getReference();

		if (varRefEnd >= refStr.length()) return ErrorWarningType.ERROR_OUT_OF_EXON;

		String variantReference = refStr.substring(varRefStart, varRefEnd + 1);

		// Reference sequence different than expected?
		if (!realReference.equals(variantReference)) { //
			return ErrorWarningType.WARNING_REF_DOES_NOT_MATCH_GENOME;
		}

		// OK
		return null;
	}


	public void setAaIdx(int aaIdxStart, int aaIdxEnd) {
		this.aaIdxStart = aaIdxStart;
		this.aaIdxEnd = aaIdxEnd;
	}

	/**
	 * Frame can be {-1, 0, 1, 2}, where '-1' means unknown
	 */
	@Override
	public void setFrame(int frame) {
		if ((frame > 2) || (frame < -1)) throw new RuntimeException("Invalid frame value: " + frame);
		this.frame = (byte) frame;
	}

	public void setRank(int rank) {
		this.rank = rank;
	}

	@Override
	public String toString() {
		switch (ToStringVersion) {
		case 1:
			// Old format version: Used in some testCases
			return getChromosomeName() + ":" + start + "-" + end //
					+ ((id != null) && (id.length() > 0) ? " '" + id + "'" : "") //
					+ " rank:" + rank //
					+ (sequence != null ? ", sequence: " + sequence : "");

		case 2:
			return getChromosomeName() + ":" + start + "-" + end //
					+ ((id != null) && (id.length() > 0) ? " '" + id + "'" : "") //
					+ ", rank: " + rank //
					+ ", frame: " + (frame >= 0 ? "" + frame : ".") //
					+ (sequence != null ? ", sequence: " + sequence : "");

		default:
			throw new RuntimeException("Unknown format version: " + ToStringVersion);
		}
	}

	/**
	 * Note: This only adds spliceSites effects, for detailed
	 *       codon changes effects we use 'CodonChange' class
	 */
	@Override
	public boolean variantEffect(Variant variant, VariantEffects variantEffects) {
		if (!intersects(variant)) return false;

		Transcript tr = (Transcript) parent;
		boolean coding = tr.isProteinCoding() || Config.get().isTreatAllAsProteinCoding();

		// Different analysis for coding or non-coding
		boolean exonAnnotated = false;
		if (!coding || variant.isInterval() || !variant.isVariant()) {
			// Non-coding? Just annotate as 'exon'
			variantEffects.add(variant, this, EffectType.EXON, "");
			exonAnnotated = true;
		} else if (tr.isCds(variant)) {
			// Is it a coding transcript and the variant is within the CDS?
			// => We need codon analysis
			CodonChange codonChange = CodonChange.factory(variant, tr, variantEffects);
			codonChange.codonChange();
			exonAnnotated = true;
		}

		// Any splice site effect to add?
		for (SpliceSite ss : spliceSites)
			if (ss.intersects(variant)) ss.variantEffect(variant, variantEffects);

		return exonAnnotated;
	}

}
