package org.bgi.flexlab.gaea.data.structure.bam.filter.util;

import org.bgi.flexlab.gaea.data.structure.region.Region;

import htsjdk.samtools.SAMRecord;

public class ReadsFilter implements SamRecordFilter {

	@Override
	public boolean filter(SAMRecord sam, Region region) {
		return FiltersMethod.filterDuplicateRead(sam)
				|| FiltersMethod.filterMappingQualityUnavailable(sam,0)
				|| FiltersMethod.filterMappingQualityUnavailable(sam,255)
				|| FiltersMethod.filterNotPrimaryAlignment(sam)
				|| FiltersMethod.filterUnmappedReads(sam)
				|| FiltersMethod.FailsVendorQualityCheckFilter(sam);
	}
}
