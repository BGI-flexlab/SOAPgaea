package org.bgi.flexlab.gaea.data.structure.reads;

public class ReadInformationWithSampleID extends ReadInformation{
	private String sampleID = "+";
	private boolean firstRead = true;
	
	public ReadInformationWithSampleID(){
		super();
	}
	
	public ReadInformationWithSampleID(String readSequence,String qualityString,String readName){
		super(readSequence,qualityString,readName);
		setFlag(readName);
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
	
	private void setFlag(String readName){
		int index = readName.lastIndexOf("/");
		if( index == -1 )
			return;
		if(!readName.substring(index+1).equals("1"))
			firstRead = false;
	}
	
	public boolean isFirstRead(){
		return this.firstRead;
	}
	
	public boolean equals(ReadInformationWithSampleID other){
		return firstRead == other.isFirstRead();
	}
	
	public String toString(int qualTrim){
		StringBuilder fastq = new StringBuilder();
		fastq.append(readName);
		fastq.append("\n");
		fastq.append(readSequence);
		fastq.append("\n");
		fastq.append(sampleID);
		fastq.append("\n");
		if (qualTrim != 0) {
			byte[] qual = qualityString.getBytes();
			for (int i = 0; i < qual.length; i++)
				qual[i] -= qualTrim;
			fastq.append(new String(qual));
		} else {
			fastq.append(qualityString);
		}
		return fastq.toString();
	}
}
