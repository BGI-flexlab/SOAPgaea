package org.bgi.flexlab.gaea.data.structure.positioninformation;

import java.util.Arrays;

public class ShortPositionInformation {
	protected short info[];
	
	public ShortPositionInformation(int windowSize) {
		info = new short[windowSize];
		Arrays.fill(info, (short)0);
	}
	
	public short get(int i) {
		return info[i];
	}
}
