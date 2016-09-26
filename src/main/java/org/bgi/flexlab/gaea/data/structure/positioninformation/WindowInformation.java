package org.bgi.flexlab.gaea.data.structure.positioninformation;

import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformationShare;
import org.bgi.flexlab.gaea.util.SAMInformationBasic;

public class WindowInformation<T extends SAMInformationBasic> extends CompoundInformation<T> {
	
	private final int windowSize;
	
	public WindowInformation(int windowStart, int winSize, T readInfo, ChromosomeInformationShare chrInfo) {
		super(windowStart, readInfo, chrInfo);
		this.windowSize = winSize;
	}
	
	public int getWindowSize(){
		return windowSize;
	}
	
}
