package org.bgi.flexlab.gaea.data.structure.bam.filter;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;

public abstract class ReadsFilter implements SamRecordFilter {

	protected SAMFileHeader mFileHeader = null;
	
	public void setHeader(SAMFileHeader mFileHeader){
		this.mFileHeader=mFileHeader;
	}

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
}
