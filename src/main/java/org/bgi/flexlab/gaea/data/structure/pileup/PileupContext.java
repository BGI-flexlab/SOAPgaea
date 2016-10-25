package org.bgi.flexlab.gaea.data.structure.pileup;

import java.util.List;

import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocation;
import org.bgi.flexlab.gaea.exception.UserException;

public class PileupContext {
	protected GenomeLocation location = null;
	protected Pileup basePileup = null;
	protected boolean hasPileupBeenDownsampled;
	private long skippedBases = 0;

	public PileupContext(GenomeLocation location, Pileup basePileup) {
		this(location, basePileup, 0, false);
	}

	public PileupContext(GenomeLocation location, Pileup basePileup,
			boolean hasPileupBeenDownsampled) {
		this(location, basePileup, 0, hasPileupBeenDownsampled);
	}

	public PileupContext(GenomeLocation location, Pileup basePileup,
			long skippedBases) {
		this(location, basePileup, skippedBases, false);
	}

	public PileupContext(GenomeLocation location, Pileup basePileup,
			long skippedBases, boolean hasPileupBeenDownsampled) {
		if (location == null)
			throw new UserException.PileupException(
					"BUG: GenomeLocation in Alignment context is null");
		if (basePileup == null)
			throw new UserException.PileupException(
					"BUG: pileup in Alignment context is null");
		if (skippedBases < 0)
			throw new UserException.PileupException(
					"BUG: skippedBases is -1 in Alignment context");

		this.location = location;
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
		return location;
	}

	/**
	 * Returns the number of bases we've skipped over in the reference since the
	 * last map invocation.
	 */
	public long getSkippedBases() {
		return skippedBases;
	}
}
