package org.bgi.flexlab.gaea.tools.haplotypecaller;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.bgi.flexlab.gaea.data.mapreduce.writable.SamRecordWritable;
import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocation;
import org.bgi.flexlab.gaea.tools.haplotypecaller.pileup.ReadsDownsampler;
import org.bgi.flexlab.gaea.tools.haplotypecaller.readfilter.ReadFilter;

import htsjdk.samtools.SAMFileHeader;

public class ReadsDataSource implements Iterator<GaeaSamRecord>, Iterable<GaeaSamRecord>{
	private GaeaSamRecord currentRecord = null;

	// reads iterator
	private Iterator<SamRecordWritable> reads;

	private List<GaeaSamRecord> overlaps = new ArrayList<GaeaSamRecord>();

	private SAMFileHeader header = null;
	
	private ReadsDownsampler downsampler;
    private Iterator<GaeaSamRecord> cachedDownsampledReads = null;
    private GaeaSamRecord nextRead = null;   
    private GenomeLocation queryInterval = null;
    private ReadFilter readFilter = null;

	public ReadsDataSource(Iterable<SamRecordWritable> iterable, SAMFileHeader header) {
		this.reads = iterable.iterator();
		this.header = header;
	}

	public void dataReset(Iterable<SamRecordWritable> iterable) {
		reads = iterable.iterator();
		if (overlaps != null && !overlaps.isEmpty())
			overlaps.clear();
		currentRecord = null;
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

	private boolean fillDownsampledReadsCache() {

		final boolean traversalIsBounded = queryInterval != null;
		
		if (overlaps != null && !overlaps.isEmpty()) {
			for(GaeaSamRecord read : overlaps){
				if(overlapReads(queryInterval , read) == 1)
					downsampler.submit(read);
			}
			
			overlaps.clear();
		}
		
		if ( !downsampler.hasFinalizedItems() ){
    		if (traversalIsBounded) {
    			while (currentRecord != null) {
    				if ((readFilter != null && readFilter.test(currentRecord)) || readFilter == null) {
    					int result = overlapReads(queryInterval , currentRecord);
    					if (result == 0) {
    						continue;
    					} else if (result == 1) {
    						downsampler.submit(currentRecord);
    					} else
    						break;
    				}
    				if(reads.hasNext())
    					currentRecord = new GaeaSamRecord(header,reads.next().get());
    				else
    					currentRecord = null;
    			}
    		}
        }

        if (! reads.hasNext() ) {
            downsampler.signalEndOfInput();
        }

        overlaps = downsampler.consumeFinalizedItems();
        cachedDownsampledReads = overlaps.iterator();
        
        downsampler.clearItems();

        return cachedDownsampledReads.hasNext();
	}

	public void clear() {
		this.overlaps = null;
		downsampler.clearItems();
		nextRead = null;
	}

	@Override
	public Iterator<GaeaSamRecord> iterator() {
		return this;
	}

	@Override
	public boolean hasNext() {
		return nextRead != null;
	}

	@Override
	public GaeaSamRecord next() {
		if ( nextRead == null ) {
            throw new NoSuchElementException("next() called when there are no more items");
        }

        final GaeaSamRecord toReturn = nextRead;
        advanceToNextRead();

        return toReturn;
	}
	
	private void advanceToNextRead() {
        if ( readyToReleaseReads() || fillDownsampledReadsCache() ) {
            nextRead = cachedDownsampledReads.next();
        }
        else {
            nextRead = null;
        }
    }
	
	public void set(GenomeLocation interval , ReadFilter readFilter,ReadsDownsampler downsampler){
		this.queryInterval = interval;
		this.readFilter = readFilter;
		this.downsampler = downsampler;
		advanceToNextRead();
	}
	
	private boolean readyToReleaseReads() {
        return cachedDownsampledReads != null && cachedDownsampledReads.hasNext();
    }
}
