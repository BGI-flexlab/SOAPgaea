package org.bgi.flexlab.gaea.data.structure.pileup.manager;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocation;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocationParser;
import org.bgi.flexlab.gaea.data.structure.pileup.Pileup;

public class PileupState {
	private Queue<GaeaSamRecord> records = null;
	private Pileup nextPileup = null;
	private Queue<SamRecordState> samRecordStates = null;
	private GenomeLocation location = null;
	private GenomeLocationParser genomeLocParser = null;
	private PileupCreator creator = null;

	public PileupState(ArrayList<GaeaSamRecord> records,
			GenomeLocationParser genomeLocParser) {
		this.records = new LinkedList<GaeaSamRecord>(records);
		samRecordStates = new LinkedList<SamRecordState>();
		this.genomeLocParser = genomeLocParser;
		creator = new PileupCreator();
	}

	public boolean hasNext() {
		lazyLoadNextAlignmentContext();
		return (nextPileup != null);
	}

	private void lazyLoadNextAlignmentContext() {
		nextPileup = null;
		if (nextPileup == null
				&& (!samRecordStates.isEmpty() || !records.isEmpty())) {
			collectPendingReads();
			nextPileup = creator.lazyLoadNextPileup(samRecordStates, location);
		} else {
			nextPileup = null;
		}
	}

	public Pileup next() {
		return nextPileup;
	}

	public void collectPendingReads() {
		if (samRecordStates.size() == 0) {
			int firstContigIndex = records.peek().getReferenceIndex();
			int firstAlignmentStart = records.peek().getAlignmentStart();
			while (!records.isEmpty()
					&& records.peek().getReferenceIndex() == firstContigIndex
					&& records.peek().getAlignmentStart() == firstAlignmentStart) {
				addReadToState(records.remove());
			}
		}
		// set loction
		location = getLocation();
		while (!records.isEmpty()) {
			if (records.peek().getReferenceIndex() == location.getContigIndex()
					&& records.peek().getAlignmentStart() == location
							.getStart()) {
				addReadToState(records.remove());
			} else {
				break;
			}

		}
	}

	private void addReadToState(GaeaSamRecord record) {
		if (record == null) {
			return;
		}
		SamRecordState state = new SamRecordState(record);
		state.stepForwardOnGenome();
		samRecordStates.add(state);
	}

	private GenomeLocation getLocation() {
		return samRecordStates.isEmpty() ? null : samRecordStates.peek()
				.getLocation(genomeLocParser);
	}
}
