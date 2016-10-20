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
	
	public boolean filterMappingQualityZero(SAMRecord read) {
		return (read.getMappingQuality() == 0);
	}
	
	public boolean filterMappingQualityUnavailable(SAMRecord read) {
		return (read.getMappingQuality() == 255);
	}
	
	public boolean FailsVendorQualityCheckFilter(SAMRecord read) {
		 return read.getReadFailsVendorQualityCheckFlag();
	}

	@Override
	public boolean filter(SAMRecord sam, Region region) {
		// TODO Auto-generated method stub
		return filterDuplicateRead(sam) 
				|| filterMappingQualityUnavailable(sam)
				|| filterMappingQualityZero(sam)
				|| filterNotPrimaryAlignment(sam)
				|| filterUnmappedReads(sam)
				|| FailsVendorQualityCheckFilter(sam);
	}
}
