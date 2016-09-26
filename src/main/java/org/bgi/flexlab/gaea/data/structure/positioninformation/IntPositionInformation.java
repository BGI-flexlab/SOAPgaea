package org.bgi.flexlab.gaea.data.structure.positioninformation;

public class IntPositionInformation {
	protected int info[];
	
	public IntPositionInformation(int windowSize) {
		info = new int[windowSize];
	}
	
	public int get(int i) {
		return info[i];
	}
}
