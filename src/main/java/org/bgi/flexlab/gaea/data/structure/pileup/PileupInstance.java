package org.bgi.flexlab.gaea.data.structure.pileup;

import java.util.List;
import java.util.Map;

import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocation;

public class PileupInstance extends AbstractPileup<PileupInstance, PileupElement> {
	public PileupInstance(GenomeLocation location) {
		super(location);
	}

	public PileupInstance(GenomeLocation location, List<GaeaSamRecord> reads,
			List<Integer> offsets) {
		super(location, reads, offsets);
	}

	public PileupInstance(GenomeLocation location, List<GaeaSamRecord> reads, int offset) {
		super(location, reads, offset);
	}

	public PileupInstance(GenomeLocation location, List<PileupElement> pileupElements) {
		super(location, pileupElements);
	}

	public PileupInstance(GenomeLocation location,
			Map<String, PileupInstance> pileupElementsBySample) {
		super(location, pileupElementsBySample);
	}

	/**
	 * Optimization of above constructor where all of the cached data is
	 * provided
	 */
	public PileupInstance(GenomeLocation location, List<PileupElement> pileup, int size,
			int nDeletions, int nMQ0Reads) {
		super(location, pileup, size, nDeletions, nMQ0Reads);
	}

	protected PileupInstance(GenomeLocation location,
			PileupElementTracker<PileupElement> tracker) {
		super(location, tracker);
	}

	@Override
	protected PileupInstance createNewPileup(GenomeLocation location,
			PileupElementTracker<PileupElement> tracker) {
		return new PileupInstance(location, tracker);
	}

	@Override
	protected PileupElement createNewPileupElement(final GaeaSamRecord read,
			final int offset, final boolean isDeletion,
			final boolean isBeforeDeletion, final boolean isAfterDeletion,
			final boolean isBeforeInsertion, final boolean isAfterInsertion,
			final boolean isNextToSoftClip) {
		return new PileupElement(read, offset, isDeletion, isBeforeDeletion,
				isAfterDeletion, isBeforeInsertion, isAfterInsertion,
				isNextToSoftClip, null, 0);
	}

	@Override
	protected PileupElement createNewPileupElement(final GaeaSamRecord read,
			final int offset, final boolean isDeletion,
			final boolean isBeforeDeletion, final boolean isAfterDeletion,
			final boolean isBeforeInsertion, final boolean isAfterInsertion,
			final boolean isNextToSoftClip, final String nextEventBases,
			final int nextEventLength) {
		return new PileupElement(read, offset, isDeletion, isBeforeDeletion,
				isAfterDeletion, isBeforeInsertion, isAfterInsertion,
				isNextToSoftClip, nextEventBases, nextEventLength);
	}
}
