package org.bgi.flexlab.gaea.data.structure.positioninformation;

import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformationShare;
import org.bgi.flexlab.gaea.util.SAMInformationBasic;

public class BamPositionInformation<T extends SAMInformationBasic> extends CompoundInformation<T>{
	
	private final int refPosition;
	
	private final int readPosition;
	
	public BamPositionInformation(T readInfo, ChromosomeInformationShare chrInfo, int windowStart,
			int refPostion, int readPosition) {
		super(windowStart, readInfo, chrInfo);
		this.refPosition = readPosition;
		this.readPosition = readPosition;
	}
	
	public int distBetweenRefPosAndWinStart() {
		return refPosition - windowStart;
	}
	
	public int getBinaryBase() {
		return readInfo.getBinaryBase(readPosition);
	}
	
	public int getRefPosition() {
		return refPosition;
	}

	public int getReadPosition() {
		return readPosition;
	}

}
