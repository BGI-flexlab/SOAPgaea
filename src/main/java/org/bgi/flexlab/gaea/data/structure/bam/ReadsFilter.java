package org.bgi.flexlab.gaea.data.structure.bam;

import org.bgi.flexlab.gaea.data.structure.region.Region;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;

public abstract class ReadsFilter implements SamRecordFilter {

	private SAMFileHeader mFileHeader = null;
	
	public ReadsFilter(SAMFileHeader mFileHeader) {
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
	
	public boolean MalformedReadFilter(SAMRecord read)
	{
		MalformedReadFilter mal=new MalformedReadFilter();
		mal.initialize(mFileHeader);
		return mal.filterOut(read);
	}
	
}
