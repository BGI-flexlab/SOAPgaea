package org.bgi.flexlab.gaea.data.structure.alignment;

import htsjdk.samtools.SAMReadGroupRecord;
import ngs.ReadGroup;
import org.apache.htrace.fasterxml.jackson.annotation.JsonTypeInfo;
import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.data.structure.bam.SAMCompressionInformationBasic;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AlignmentsBasic extends SAMCompressionInformationBasic {
	protected static Map<Integer, String> Id2Sample = new HashMap<Integer, String>();

	protected int sampleIndex;

	public AlignmentsBasic() {
		super();
	}

	public void initId2Sample(List<SAMReadGroupRecord> samReadGroupRecords) {
		int i = 0;
		for(SAMReadGroupRecord samReadGroupRecord : samReadGroupRecords) {
			Id2Sample.put(i, samReadGroupRecord.getSample());
		}
	}

	public void parseOtherInfo(GaeaSamRecord samRecord) {
	}

	public String getSample() {
		return Id2Sample.get(sampleIndex);
	}

	public int getSampleIndex() {
		return sampleIndex;
	}

	public void setSampleIndex(int sampleIndex) {
		this.sampleIndex = sampleIndex;
	}
}

