package org.bgi.flexlab.gaea.data.structure.bam;

import htsjdk.samtools.SAMRecord;

public interface SamRecordFilter {
	
	public boolean filter(SAMRecord sam);
	
	public class DefaultSamRecordFilter implements SamRecordFilter{

		@Override
		public boolean filter(SAMRecord sam) {
			return false;
		}
	}
}
