package org.bgi.flexlab.gaea.data.structure.bam.filter.util;

import org.bgi.flexlab.gaea.data.structure.region.Region;

import htsjdk.samtools.SAMRecord;

public interface SamRecordFilter {
	
	public boolean filter(SAMRecord sam,Region region);
	
	public class DefaultSamRecordFilter implements SamRecordFilter{

		@Override
		public boolean filter(SAMRecord sam,Region region) {
			return false;
		}
	}
}
