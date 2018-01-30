package org.bgi.flexlab.gaea.tools.haplotypecaller;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocation;

public class ReadsDataSource {

	private List<GaeaSamRecord> reads = null;

	public ReadsDataSource(List<GaeaSamRecord> inputReads) {
		this.reads = inputReads;
	}

	public Iterator<GaeaSamRecord> query(final GenomeLocation interval) {
		return prepareIteratorsForTraversal(Arrays.asList(interval));
	}

	private Iterator<GaeaSamRecord> prepareIteratorsForTraversal(final List<GenomeLocation> queryIntervals) {
		return prepareIteratorsForTraversal(queryIntervals, false);
	}

	private Iterator<GaeaSamRecord> prepareIteratorsForTraversal(final List<GenomeLocation> queryIntervals,
			final boolean queryUnmapped) {

		final boolean traversalIsBounded = (queryIntervals != null && !queryIntervals.isEmpty()) || queryUnmapped;

		List<GaeaSamRecord> overlaps = new ArrayList<GaeaSamRecord>();
		if (traversalIsBounded) {
			for (GaeaSamRecord read : reads) {
				for (GenomeLocation interval : queryIntervals) {
					if ((read.getAlignmentStart() >= interval.getStart()
							&& read.getAlignmentStart() <= interval.getEnd())
							|| (read.getAlignmentEnd() >= interval.getStart()
									&& read.getAlignmentEnd() <= interval.getEnd()))
						overlaps.add(read);
				}
			}
		} else {
			return reads.iterator();
		}

		return overlaps.iterator();
	}
	
	public void clear(){
		this.reads.clear();
	}
}
