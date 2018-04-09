package org.bgi.flexlab.gaea.tools.haplotypecaller;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.bgi.flexlab.gaea.data.mapreduce.writable.SamRecordWritable;
import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocation;

import htsjdk.samtools.SAMFileHeader;

public class ReadsDataSource {
	private GaeaSamRecord currentRecord = null;

	// reads iterator
	private Iterable<SamRecordWritable> reads;
	
	private List<GaeaSamRecord> overlaps = new ArrayList<GaeaSamRecord>();
	
	private SAMFileHeader header = null;

	public ReadsDataSource(Iterable<SamRecordWritable> iterable,SAMFileHeader header) {
		this.reads = iterable;
		this.header = header;
	}
	
	public void dataReset(Iterable<SamRecordWritable> iterable){
		reads = iterable;
		if(!overlaps.isEmpty())
			overlaps.clear();
	}

	public Iterator<GaeaSamRecord> query(final GenomeLocation interval) {
		return prepareIteratorsForTraversal(interval,false);
	}

	private Iterator<GaeaSamRecord> prepareIteratorsForTraversal(final GenomeLocation queryInterval,
			final boolean queryUnmapped) {

		final boolean traversalIsBounded = queryInterval != null || queryUnmapped;

		if(!overlaps.isEmpty()){
			List<GaeaSamRecord> remove = new ArrayList<GaeaSamRecord>();
			for(GaeaSamRecord read : overlaps){
				if(read.getEnd() < queryInterval.getStart())
					remove.add(read);
				else if(read.getStart() >= queryInterval.getStart())
					break;
			}
			overlaps.removeAll(remove);
			remove.clear();
		}
		
		if(traversalIsBounded){
			for(SamRecordWritable srw : reads){
				currentRecord = new GaeaSamRecord(header,srw.get());
				if(currentRecord.getEnd() < queryInterval.getStart())
					continue;
				else if((currentRecord.getAlignmentStart() >= queryInterval.getStart()
						&& currentRecord.getAlignmentStart() <= queryInterval.getEnd())
						|| (currentRecord.getAlignmentEnd() >= queryInterval.getStart()
								&& currentRecord.getAlignmentEnd() <= queryInterval.getEnd())){
					overlaps.add(currentRecord);
				}else
					break;
			}
		}

		return overlaps.iterator();
	}
	
	public void clear(){
		this.reads = null;
	}
}
