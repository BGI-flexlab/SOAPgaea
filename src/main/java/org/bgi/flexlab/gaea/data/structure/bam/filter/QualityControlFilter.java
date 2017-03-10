package org.bgi.flexlab.gaea.data.structure.bam.filter;

import htsjdk.samtools.SAMRecord;

import org.bgi.flexlab.gaea.data.structure.bam.filter.util.FiltersMethod;
import org.bgi.flexlab.gaea.data.structure.bam.filter.util.MalformedReadFilter;
import org.bgi.flexlab.gaea.data.structure.bam.filter.util.ReadsFilter;
import org.bgi.flexlab.gaea.data.structure.bam.filter.util.SamRecordFilter;
import org.bgi.flexlab.gaea.data.structure.region.Region;

public class QualityControlFilter implements SamRecordFilter {
	private ReadsFilter readsFilter = new ReadsFilter();
	private MalformedReadFilter malformedReadFilter = new MalformedReadFilter();

	@Override
	public boolean filter(SAMRecord sam, Region region) {
		return FiltersMethod.filterBadCigar(sam) || FiltersMethod.filterBadMate(sam)
				|| readsFilter.filter(sam, region)
				|| malformedReadFilter.filter(sam, region);
	}
}
