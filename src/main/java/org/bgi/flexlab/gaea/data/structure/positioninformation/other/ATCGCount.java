package org.bgi.flexlab.gaea.data.structure.positioninformation.other;

public class ATCGCount {
	short[] posATCG = new short[4];
	
	public void add(int count, int index) {
		posATCG[index] += count;
	}
	
	public short get(int index) {
		return posATCG[index];
	}
}
