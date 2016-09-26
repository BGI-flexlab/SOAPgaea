package org.bgi.flexlab.gaea.util;

public class FastqInformation extends FastqInformationBasic{
	protected String readName;
	
	public FastqInformation() {
		super();
		readName = "";
	}
	
	public FastqInformation(String readName, String seq, String qual) {
		super(seq, qual);
		this.readName = readName;
	}

	public String getReadName() {
		return readName;
	}
}
