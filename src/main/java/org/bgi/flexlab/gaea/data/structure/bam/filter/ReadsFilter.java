package org.bgi.flexlab.gaea.data.structure.bam.filter;

import org.bgi.flexlab.gaea.data.structure.region.Region;

import htsjdk.samtools.SAMRecord;

public class ReadsFilter implements SamRecordFilter {

	public boolean filterUnmappedReads(SAMRecord read) {
		return read.getReadUnmappedFlag() || read.getAlignmentStart() == SAMRecord.NO_ALIGNMENT_START;
	}
	
	public boolean filterNotPrimaryAlignment(SAMRecord read) {
		return read.getNotPrimaryAlignmentFlag();
	}
	
	public boolean filterDuplicateRead(SAMRecord read) {
		return read.getDuplicateReadFlag();
	}
	
	public boolean filterMappingQualityUnavailable(SAMRecord read,int unavailableQuality) {
		return (read.getMappingQuality() == unavailableQuality);
	}
	
	public boolean FailsVendorQualityCheckFilter(SAMRecord read) {
		 return read.getReadFailsVendorQualityCheckFlag();
	}

	@Override
	public boolean filter(SAMRecord sam, Region region) {
		return filterDuplicateRead(sam) 
				|| filterMappingQualityUnavailable(sam,0)
				|| filterMappingQualityUnavailable(sam,255)
				|| filterNotPrimaryAlignment(sam)
				|| filterUnmappedReads(sam)
				|| FailsVendorQualityCheckFilter(sam);
	}
}
