package org.bgi.flexlab.gaea.tools.haplotypecaller;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.bgi.flexlab.gaea.data.mapreduce.writable.SamRecordWritable;
import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocation;
import org.bgi.flexlab.gaea.tools.haplotypecaller.readfilter.ReadFilter;

import htsjdk.samtools.SAMFileHeader;

public class ReadsDataSource {
	private GaeaSamRecord currentRecord = null;

	// reads iterator
	private Iterable<SamRecordWritable> reads;

	private List<GaeaSamRecord> overlaps = new ArrayList<GaeaSamRecord>();

	private SAMFileHeader header = null;

	public ReadsDataSource(Iterable<SamRecordWritable> iterable, SAMFileHeader header) {
		this.reads = iterable;
		this.header = header;
	}

	public void dataReset(Iterable<SamRecordWritable> iterable) {
		reads = iterable;
		if (!overlaps.isEmpty())
			overlaps.clear();
		currentRecord = null;
	}

	public Iterator<GaeaSamRecord> query(final GenomeLocation interval) {
		return prepareIteratorsForTraversal(interval, false,null);
	}

	private Iterator<GaeaSamRecord> prepareIteratorsForTraversal(final GenomeLocation queryInterval,
			final boolean queryUnmapped) {

		final boolean traversalIsBounded = queryInterval != null || queryUnmapped;

		if (!overlaps.isEmpty()) {
			List<GaeaSamRecord> remove = new ArrayList<GaeaSamRecord>();
			for (GaeaSamRecord read : overlaps) {
				if (read.getEnd() < queryInterval.getStart())
					remove.add(read);
				else if (read.getStart() >= queryInterval.getStart())
					break;
			}
			overlaps.removeAll(remove);
			remove.clear();
		}

		if (traversalIsBounded) {
			if(currentRecord != null) {
				if ((currentRecord.getAlignmentStart() >= queryInterval.getStart()
						&& currentRecord.getAlignmentStart() <= queryInterval.getEnd())
						|| (currentRecord.getAlignmentEnd() >= queryInterval.getStart()
								&& currentRecord.getAlignmentEnd() <= queryInterval.getEnd())) {
					overlaps.add(currentRecord);
				}
			}
			for (SamRecordWritable srw : reads) {
				currentRecord = new GaeaSamRecord(header, srw.get());
				if (currentRecord.getEnd() < queryInterval.getStart())
					continue;
				else if ((currentRecord.getAlignmentStart() >= queryInterval.getStart()
						&& currentRecord.getAlignmentStart() <= queryInterval.getEnd())
						|| (currentRecord.getAlignmentEnd() >= queryInterval.getStart()
								&& currentRecord.getAlignmentEnd() <= queryInterval.getEnd())) {
					overlaps.add(currentRecord);
				} else
					break;
			}
		}

		return overlaps.iterator();
	}

	public Iterator<GaeaSamRecord> query(final GenomeLocation interval, final ReadFilter readFilter) {
		return prepareIteratorsForTraversal(interval, false, readFilter);
	}
	
	private int overlapReads(final GenomeLocation queryInterval,GaeaSamRecord read) {
		if (read.getEnd() < queryInterval.getStart()) {
			return 0;
		} else if ((read.getAlignmentStart() >= queryInterval.getStart()
				&& read.getAlignmentStart() <= queryInterval.getEnd())
				|| (read.getAlignmentEnd() >= queryInterval.getStart()
						&& read.getAlignmentEnd() <= queryInterval.getEnd())) {
			return 1;
		} else
			return 2;
	}

	private Iterator<GaeaSamRecord> prepareIteratorsForTraversal(final GenomeLocation queryInterval,
			final boolean queryUnmapped, final ReadFilter readFilter) {

		final boolean traversalIsBounded = queryInterval != null || queryUnmapped;

		if (!overlaps.isEmpty()) {
			List<GaeaSamRecord> remove = new ArrayList<GaeaSamRecord>();
			for (GaeaSamRecord read : overlaps) {
				if (read.getEnd() < queryInterval.getStart())
					remove.add(read);
				else if (read.getStart() >= queryInterval.getStart())
					break;
			}
			overlaps.removeAll(remove);
			remove.clear();
		}

		if (traversalIsBounded) {
			if(currentRecord != null) {
				if(readFilter == null) {
					if(overlapReads(queryInterval , currentRecord) == 1)
						overlaps.add(currentRecord);
				}else {
					if(readFilter.test(currentRecord) && overlapReads(queryInterval , currentRecord) == 1)
						overlaps.add(currentRecord);
				}
			}
			for (SamRecordWritable srw : reads) {
				currentRecord = new GaeaSamRecord(header, srw.get());
				if ((readFilter != null && readFilter.test(currentRecord)) || readFilter == null) {
					int result = overlapReads(queryInterval , currentRecord);
					if (result == 0) {
						continue;
					} else if (result == 1) {
						overlaps.add(currentRecord);
					} else
						break;
				}
			}
		}

		return overlaps.iterator();
	}

	public void clear() {
		this.overlaps = null;
	}
}
