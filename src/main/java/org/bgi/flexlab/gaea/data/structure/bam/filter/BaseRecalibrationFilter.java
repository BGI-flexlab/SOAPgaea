package org.bgi.flexlab.gaea.data.structure.bam.filter;

import org.bgi.flexlab.gaea.data.structure.region.Region;

import htsjdk.samtools.SAMRecord;

public class BaseRecalibrationFilter implements SamRecordFilter{
	
	ReadsFilter readsFilter = new ReadsFilter();

	MalformedReadFilter malformedReadFilter = new MalformedReadFilter();
	
	@Override
	public boolean filter(SAMRecord sam, Region region) {
		return readsFilter.filter(sam, region) || malformedReadFilter.filter(sam, region);
	}
}
