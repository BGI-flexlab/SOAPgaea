package org.bgi.flexlab.gaea.util;

import htsjdk.samtools.SAMFileHeader;

public class Window {
	private String contigName;
	private int chrIndex;
	private int start;
	private int stop;
	
	public Window(int start,int stop) {
		this.start=start;
		this.stop=stop;
	}
	
	public Window(String contigName,int index,int start,int stop) {
		this.contigName=contigName;
		this.chrIndex = index;
		this.start=start;
		this.stop=stop;
	}

	public Window(SAMFileHeader mHeader, int chrIndex, int winNum, int WindowsSize) {
		int winSize = WindowsSize;
		int start = winNum * winSize;

		if (mHeader.getSequence(chrIndex) == null)
			throw new RuntimeException(String.format("chr index %d is not found in reference", chrIndex));
		String chrName = mHeader.getSequence(chrIndex).getSequenceName();
		int stop = (winNum + 1) * winSize - 1 < mHeader.getSequence(chrName).getSequenceLength()
				? (winNum + 1) * winSize - 1 : mHeader.getSequence(chrName).getSequenceLength();

		this.contigName=chrName;
		this.chrIndex = chrIndex;
		this.start=start;
		this.stop=stop;
	}
	
	public Window(){}
	
	public String getContigName() {
		return contigName;
	}
	public void setContigName(String contigName) {
		this.contigName = contigName;
	}
	
	public int getStart() {
		return start;
	}
	
	public void setStart(int start) {
		this.start = start;
	}
	
	public int getStop() {
		return stop;
	}
	public void setStop(int stop) {
		this.stop = stop;
	}
	@Override
	public String toString()
	{
		StringBuilder sb=new StringBuilder();
		if(contigName!=null) {
			sb.append(contigName);
			sb.append(":");
		}
		sb.append(start);
		sb.append("-");
		sb.append(stop);
		return sb.toString();
	}
	
	public int getChrIndex(){
		return this.chrIndex;
	}
}
