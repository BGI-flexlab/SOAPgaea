package org.bgi.flexlab.gaea.data.structure.bam.filter;

import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.data.structure.bam.filter.util.ReadsFilter;
import org.bgi.flexlab.gaea.util.ReadUtils;

import htsjdk.samtools.SAMRecord;

public class RealignerFilter extends ReadsFilter {

	public static boolean needToClean(GaeaSamRecord read,int maxInsertSize) {
		return read.getReadUnmappedFlag() || read.getNotPrimaryAlignmentFlag()
				|| read.getReadFailsVendorQualityCheckFlag()
				|| read.getMappingQuality() == 0
				|| read.getAlignmentStart() == SAMRecord.NO_ALIGNMENT_START
				|| iSizeTooBigToMove(read, maxInsertSize)
				|| ReadUtils.is454Read(read) || ReadUtils.isIonRead(read);
	}

	public static boolean iSizeTooBigToMove(GaeaSamRecord read,
			int maxInsertSizeForMovingReadPairs) {
		return (read.getReadPairedFlag() && !read.getMateUnmappedFlag() && !read
				.getReferenceName().equals(read.getMateReferenceName())) 
				|| Math.abs(read.getInferredInsertSize()) > maxInsertSizeForMovingReadPairs; 
	}
}
