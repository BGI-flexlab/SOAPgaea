package org.bgi.flexlab.gaea.util;

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
