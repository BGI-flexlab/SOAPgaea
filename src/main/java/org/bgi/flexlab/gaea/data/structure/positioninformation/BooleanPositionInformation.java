package org.bgi.flexlab.gaea.data.structure.positioninformation;

public class BooleanPositionInformation {
	protected boolean info[];
	
	public BooleanPositionInformation(int windowSize) {
		info = new boolean[windowSize];
	}
	
	public boolean get(int i) {
		return info[i];
	}
}
