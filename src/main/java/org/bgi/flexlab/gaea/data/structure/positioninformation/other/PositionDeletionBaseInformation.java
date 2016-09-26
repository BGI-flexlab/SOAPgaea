package org.bgi.flexlab.gaea.data.structure.positioninformation.other;

import org.bgi.flexlab.gaea.data.structure.positioninformation.BooleanPositionInformation;

public class PositionDeletionBaseInformation extends BooleanPositionInformation {

	public PositionDeletionBaseInformation(int windowSize) {
		super(windowSize);
		//Arrays.fill(info, true);
	}
	
	public void addDeleteBase(int depth, int winIndex) {
		if(depth > 0) {
			info[winIndex] = false;
		} else {
			info[winIndex] = true;
		}
	}

}
