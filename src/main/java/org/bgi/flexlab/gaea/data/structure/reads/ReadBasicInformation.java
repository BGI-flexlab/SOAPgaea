package org.bgi.flexlab.gaea.data.structure.reads;

public class ReadBasicInformation {
	protected String readSequence;
	protected String qualityString;
	protected int MINIMUM_BASE_QUALITY = 33;

	public ReadBasicInformation() {
		this.readSequence = null;
		this.qualityString = null;
	}

	public ReadBasicInformation(String readSequence, String qualityString) {
		this.readSequence = readSequence;
		this.qualityString = qualityString;
	}
	
	public void setMinimumBaseQuality(int minimumBaseQuality) {
		MINIMUM_BASE_QUALITY = minimumBaseQuality;
	}

	public int getReadLength() {
		return readSequence.length();
	}

	public char getBaseFromRead(int position) {
		return readSequence.charAt(position);
	}

	public String getReadsSequence() {
		return readSequence;
	}

	public char getBaseQuality(int position) {
		return qualityString.charAt(position);
	}
	
	public int getBaseQualityValue(int position){
		return (int)getBaseQuality(position) - MINIMUM_BASE_QUALITY;
	}

	public byte getBinaryBase(int i) {
		return (byte) ((readSequence.charAt(i) >> 1) & 0x07);
	}
	
	public String getQualityString() {
		return qualityString;
	}
	
	public String getQualityValue(){
		StringBuilder sb = new StringBuilder();
		for(int i=0 ;i<qualityString.length();i++){
			sb.append(getBaseQualityValue(i));
		}
		return sb.toString();
	}
}
