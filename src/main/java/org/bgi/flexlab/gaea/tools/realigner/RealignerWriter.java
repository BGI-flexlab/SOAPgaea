package org.bgi.flexlab.gaea.tools.realigner;

import java.util.ArrayList;

import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;

public abstract class RealignerWriter {
	public abstract void write(GaeaSamRecord read);
	
	public abstract void close();
	
	public void writeRead(GaeaSamRecord read){
		if(read.needToOutput()){
			write(read);
		}
	}
	
	public void writeReadList(ArrayList<GaeaSamRecord> list){
		for(GaeaSamRecord read : list)
			writeRead(read);
	}
}
