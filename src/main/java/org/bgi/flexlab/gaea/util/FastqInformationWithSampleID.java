package org.bgi.flexlab.gaea.util;

public class FastqInformationWithSampleID extends FastqInformation{
	private int sampleID = 0;
	
	public FastqInformationWithSampleID(int sampleID, String readName, String seq, String qual) {
		super(readName, seq, qual);
		this.sampleID = sampleID;
	}

	public int getSampleID() {
		return sampleID;
	}
}
