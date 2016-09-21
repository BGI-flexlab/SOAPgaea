package org.bgi.flexlab.gaea.data.structure.context;

import java.util.List;

import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocation;
import org.bgi.flexlab.gaea.data.structure.pileup.Pileup;
import org.bgi.flexlab.gaea.exception.UserException;

public class AlignmentContext {
	protected GenomeLocation loc = null;
	protected Pileup basePileup = null;
	protected boolean hasPileupBeenDownsampled;

	/**
	 * The number of bases we've skipped over in the reference since the last
	 * map invocation.
	 */
	private long skippedBases = 0;

	public AlignmentContext(GenomeLocation loc, Pileup basePileup) {
		this(loc, basePileup, 0, false);
	}

	public AlignmentContext(GenomeLocation loc, Pileup basePileup,
			boolean hasPileupBeenDownsampled) {
		this(loc, basePileup, 0, hasPileupBeenDownsampled);
	}

	public AlignmentContext(GenomeLocation loc, Pileup basePileup,
			long skippedBases) {
		this(loc, basePileup, skippedBases, false);
	}

	public AlignmentContext(GenomeLocation loc, Pileup basePileup,
			long skippedBases, boolean hasPileupBeenDownsampled) {
		if (loc == null)
			throw new UserException.PileupException(
					"BUG: GenomeLocation in Alignment context is null");
		if (basePileup == null)
			throw new UserException.PileupException(
					"BUG: pileup in Alignment context is null");
		if (skippedBases < 0)
			throw new UserException.PileupException(
					"BUG: skippedBases is -1 in Alignment context");

		this.loc = loc;
		this.basePileup = basePileup;
		this.skippedBases = skippedBases;
		this.hasPileupBeenDownsampled = hasPileupBeenDownsampled;
	}

	/**
	 * Returns base pileup over the current genomic location. May return null if
	 * this context keeps only extended event (indel) pileup.
	 */
	public Pileup getBasePileup() {
		return basePileup;
	}

	/**
	 * Returns true if any reads have been filtered out of the pileup due to
	 * excess DoC.
	 */
	public boolean hasPileupBeenDownsampled() {
		return hasPileupBeenDownsampled;
	}

	/**
	 * get all of the reads within this context
	 */
	public List<GaeaSamRecord> getReads() {
		return (basePileup.getReads());
	}

	/**
	 * Are there any reads associated with this locus?
	 */
	public boolean hasReads() {
		return basePileup != null && basePileup.getNumberOfElements() > 0;
	}

	/**
	 * reads depth
	 */
	public int size() {
		return basePileup.getNumberOfElements();
	}

	public String getContig() {
		return getLocation().getContig();
	}

	public long getPosition() {
		return getLocation().getStart();
	}

	public GenomeLocation getLocation() {
		return loc;
	}

	/**
	 * Returns the number of bases we've skipped over in the reference since the
	 * last map invocation.
	 */
	public long getSkippedBases() {
		return skippedBases;
	}
}
