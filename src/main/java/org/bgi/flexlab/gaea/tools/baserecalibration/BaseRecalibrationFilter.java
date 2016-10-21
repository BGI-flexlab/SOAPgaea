package org.bgi.flexlab.gaea.tools.baserecalibration;

import org.bgi.flexlab.gaea.data.structure.bam.filter.MalformedReadFilter;
import org.bgi.flexlab.gaea.data.structure.bam.filter.ReadsFilter;
import org.bgi.flexlab.gaea.data.structure.bam.filter.SamRecordFilter;
import org.bgi.flexlab.gaea.data.structure.region.Region;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;

public class BaseRecalibrationFilter implements SamRecordFilter{
	
	SAMFileHeader mFileHeader;
	
	ReadsFilter filter1 = new ReadsFilter();

	MalformedReadFilter filter2 = new MalformedReadFilter();
	@Override
	public boolean filter(SAMRecord sam, Region region) {
		// TODO Auto-generated method stub
		return filter1.filter(sam, region) || filter2.filter(sam, region);
	}

}
