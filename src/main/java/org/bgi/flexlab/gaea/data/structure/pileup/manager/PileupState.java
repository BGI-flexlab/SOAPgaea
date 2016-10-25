package org.bgi.flexlab.gaea.data.structure.pileup.manager;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.data.structure.context.AlignmentContext;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocation;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocationParser;

public class PileupState {
	/**
	 * samrecord list
	 */
	private Queue<GaeaSamRecord> records = null;

	/**
	 * alignment context for pileup
	 */
	AlignmentContext nextAlignmentContext = null;

	/**
	 * read state
	 */
	Queue<SamRecordState> readStates = null;
	/**
	 * 
	 * Location
	 */
	private GenomeLocation location = null;

	/**
	 * GenomicParse
	 */
	GenomeLocationParser genomeLocParser = null;

	private PileupToContext readsUtil = null;

	public PileupState(ArrayList<GaeaSamRecord> records,
			GenomeLocationParser genomeLocParser) {
		this.records = new LinkedList<GaeaSamRecord>(records);
		readStates = new LinkedList<SamRecordState>();
		this.genomeLocParser = genomeLocParser;
		readsUtil = new PileupToContext();
	}

	public boolean hasNext() {
		lazyLoadNextAlignmentContext();
		return (nextAlignmentContext != null);
	}

	private void lazyLoadNextAlignmentContext() {
		if (nextAlignmentContext == null
				&& (!readStates.isEmpty() || !records.isEmpty())) {
			collectPendingReads();

			nextAlignmentContext = readsUtil.lazyLoadNextAlignmentContext(
					readStates, location);
		} else {
			nextAlignmentContext = null;
		}
	}

	public AlignmentContext next() {
		AlignmentContext currentAlignmentContext = nextAlignmentContext;
		nextAlignmentContext = null;
		return currentAlignmentContext;
	}

	public void collectPendingReads() {
		if (readStates.size() == 0) {
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
		readStates.add(state);
	}

	private GenomeLocation getLocation() {
		return readStates.isEmpty() ? null : readStates.peek().getLocation(
				genomeLocParser);
	}
}
