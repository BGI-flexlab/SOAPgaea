package org.bgi.flexlab.gaea.data.structure.bam.filter;

import htsjdk.samtools.SAMRecord;
import org.bgi.flexlab.gaea.data.structure.bam.filter.util.FiltersMethod;
import org.bgi.flexlab.gaea.data.structure.bam.filter.util.MalformedReadFilter;
import org.bgi.flexlab.gaea.data.structure.bam.filter.util.ReadsFilter;
import org.bgi.flexlab.gaea.data.structure.bam.filter.util.SamRecordFilter;
import org.bgi.flexlab.gaea.data.structure.region.Region;

/**
 * Created by zhangyong on 2017/3/10.
 */
public class GenotyperFilter implements SamRecordFilter{
    ReadsFilter readsFilter = new ReadsFilter();

    MalformedReadFilter malformedReadFilter = new MalformedReadFilter();

    @Override
    public boolean filter(SAMRecord sam, Region region) {
        return (malformedReadFilter.filter(sam, region) || readsFilter.filter(sam, region) ||
                FiltersMethod.filterBadMate(sam) || !region.isSamRecordInRegion(sam));
    }
}
