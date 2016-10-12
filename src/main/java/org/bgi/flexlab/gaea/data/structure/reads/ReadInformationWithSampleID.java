package org.bgi.flexlab.gaea.data.structure.reads;

public class ReadInformationWithSampleID extends ReadInformation{
	private String sampleID = "+";
	
	public ReadInformationWithSampleID(){
		super();
	}
	
	public ReadInformationWithSampleID(String readSequence,String qualityString,String readName){
		super(readSequence,qualityString,readName);
	}
	
	public ReadInformationWithSampleID(String[] reads){
		this(reads[1],reads[3],reads[0]);
		this.sampleID = reads[2];
	}
	
	public String getSampleID(){
		return sampleID;
	}
	
	public void set(String[] reads){
		this.readName = reads[0];
		this.readSequence = reads[1];
		this.sampleID = reads[2];
		this.qualityString = reads[3];
	}
	
	public void setReadSequence(String seq){
		this.readSequence = seq;
	}
	
	public void setReadQuality(String qual){
		this.qualityString = qual;
	}
}
