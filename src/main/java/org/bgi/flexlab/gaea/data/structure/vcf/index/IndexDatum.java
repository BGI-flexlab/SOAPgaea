package org.bgi.flexlab.gaea.data.structure.vcf.index;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import org.apache.commons.math3.util.Pair;

public class IndexDatum {
	
	private String chr;
	
	private int winID;
	
	private long start;
	
	private long end;

	private Map<Integer, Pair<Long, Long>> id2interval;
	
	
	public void write(OutputStream os) {
		StringBuilder builder = new StringBuilder();
		builder.append(chr);builder.append("\t");
		builder.append(winID);builder.append("\t");
		builder.append(start);builder.append("\t");
		builder.append(end);builder.append("\n");
		try {
			os.write(builder.toString().getBytes());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void read(String line) {
		String[] info = line.split("\t");
		setChr(info[0]);
		setWinID(Integer.parseInt(info[1]));
		setStart(Long.parseLong(info[2]));
		setEnd(Long.parseLong(info[3]));
		setId2interval();
	}
	
	public void setId2interval() {
		id2interval.put(winID, new Pair<>(start, end));
	}
	
	public String getChr() {
		return chr;
	}

	public void setChr(String chr) {
		this.chr = chr;
	}

	public int getWinID() {
		return winID;
	}

	public void setWinID(int winID) {
		this.winID = winID;
	}

	public long getStart() {
		return start;
	}

	public long getStart(int winID) {
		return id2interval.get(winID).getFirst();
	}
	
	public long getEnd(int winID) {
		return id2interval.get(winID).getSecond();
	}
	
	public void setStart(long sPos) {
		this.start = sPos;
	}

	public long getEnd() {
		return end;
	}

	public void setEnd(long ePos) {
		this.end = ePos;
	}
	

	public void setPos(long pos) {
		if(start == 0 && end == 0)
			start = pos;
		if(start !=0 && end ==0)
			end = pos;
	}
}
