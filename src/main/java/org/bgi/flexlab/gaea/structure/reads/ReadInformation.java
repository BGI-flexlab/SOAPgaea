package org.bgi.flexlab.gaea.structure.reads;

public class ReadInformation extends ReadBasicInformation{
	protected String readName;

	public ReadInformation() {
		super();
		readName = null;
	}

	public ReadInformation(String readSequence, String qualityString,
			String readName) {
		super(readSequence,qualityString);
		this.readName = readName;
	}
	
	public String getReadName(){
		return readName;
	}
}