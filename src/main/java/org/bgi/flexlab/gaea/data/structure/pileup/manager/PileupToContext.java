package org.bgi.flexlab.gaea.data.structure.pileup.manager;

import htsjdk.samtools.CigarElement;
import htsjdk.samtools.CigarOperator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;

import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocation;
import org.bgi.flexlab.gaea.data.structure.pileup.PileupContext;
import org.bgi.flexlab.gaea.data.structure.pileup.PileupElement;
import org.bgi.flexlab.gaea.data.structure.pileup.PileupImpl;
import org.bgi.flexlab.gaea.util.ReadUtils;

public class PileupToContext {
	public int getDownsamplingExtent() {
		return 0;
	}

	private static boolean filterBaseInRead(GaeaSamRecord rec, long pos) {
		return ReadUtils.isBaseInsideAdaptor(rec, pos);
	}

	private void updateReadStates(Queue<SamRecordState> readStates) {
		Iterator<SamRecordState> it = readStates.iterator();
		while (it.hasNext()) {
			SamRecordState state = it.next();
			CigarOperator op = state.stepForwardOnGenome();
			if (op == null) {
				it.remove(); // we've stepped off the end of the object
			}
		}
	}

	public PileupContext lazyLoadNextAlignmentContext(
			Queue<SamRecordState> readStates, GenomeLocation location) {
		PileupContext nextAlignmentContext = null;
		boolean hasBeenSampled = false;
		final List<PileupElement> pile = new ArrayList<PileupElement>(
				readStates.size());
		hasBeenSampled |= location.getStart() <= getDownsamplingExtent();
		int size = 0; // number of elements in this sample's pileup
		int nDeletions = 0; // number of deletions in this sample's pileup
		int nMQ0Reads = 0; // number of MQ0 reads in this sample's pileup
		for (SamRecordState state : readStates) {
			// state object with the read/offset informatio
			final GaeaSamRecord read = state.getRead(); // the actual read

			final CigarOperator op = state.getCurrentCigarElement().getOperator(); 
			final CigarElement nextElement = state.getNextCigarElement(); 
			final CigarElement lastElement = state.getLastCigarElement(); 

			final boolean isSingleElementCigar = nextElement == lastElement;
			final CigarOperator nextOp = nextElement.getOperator(); 
			final CigarOperator lastOp = lastElement.getOperator();

			int readOffset = state.getReadOffset(); // the base offset on this
													// read

			PileupCigarState cigarState = new PileupCigarState(nextOp, lastOp,
					isSingleElementCigar);

			final boolean isNextToSoftClip = nextOp == CigarOperator.S
					|| (state.getGenomeOffset() == 0 && read.getSoftStart() != read
							.getAlignmentStart());

			cigarState.setNextToSoftClip(isNextToSoftClip);

			int nextElementLength = nextElement.getLength();
			if (op == CigarOperator.N) // N's are never added to any pileup
				continue;

			if (op == CigarOperator.D) {
				cigarState.setDeletion(true);
				pile.add(new PileupElement(read, readOffset, cigarState, null,
						nextOp == CigarOperator.D ? nextElementLength : -1));
				size++;
				nDeletions++;
				if (read.getMappingQuality() == 0)
					nMQ0Reads++;
			} else {
				if (!filterBaseInRead(read, location.getStart())) {
					String insertedBaseString = null;
					if (nextOp == CigarOperator.I) {
						final int insertionOffset = isSingleElementCigar ? 0
								: 1;
						// someone please implement a better fix for
						// the single element insertion CIGAR!
						if (isSingleElementCigar)
							readOffset -= (nextElement.getLength() - 1); 

						insertedBaseString = new String(Arrays.copyOfRange(
								read.getReadBases(),
								readOffset + insertionOffset,
								readOffset + insertionOffset
										+ nextElement.getLength()));
					}

					cigarState.setDeletion(false);
					pile.add(new PileupElement(read, readOffset, cigarState,
							insertedBaseString, nextElementLength));
					size++;
					if (read.getMappingQuality() == 0)
						nMQ0Reads++;
				}
			}
		}// end ? for (SamRecordState state : readStates)
		nextAlignmentContext = new PileupContext(location, new PileupImpl(
				location, pile, size, nDeletions, nMQ0Reads), hasBeenSampled);
		updateReadStates(readStates);
		return nextAlignmentContext;
	}
}
