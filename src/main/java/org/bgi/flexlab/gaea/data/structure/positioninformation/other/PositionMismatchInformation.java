package org.bgi.flexlab.gaea.data.structure.positioninformation.other;

import org.bgi.flexlab.gaea.data.structure.positioninformation.BooleanPositionInformation;
import org.bgi.flexlab.gaea.data.structure.positioninformation.CalculatePositionInforamtionInterface;
import org.bgi.flexlab.gaea.util.SAMInformationBasic;
import org.bgi.flexlab.gaea.data.structure.positioninformation.BamPositionInformation;

public class PositionMismatchInformation extends BooleanPositionInformation implements CalculatePositionInforamtionInterface<SAMInformationBasic>{

	public PositionMismatchInformation(int windowSize) {
		super(windowSize);
	}

	@Override
	public void add(@SuppressWarnings("rawtypes") BamPositionInformation posInfo) {
		if(eligiblePos(posInfo)) {
			info[posInfo.distBetweenRefPosAndWinStart()] = true;
		}
	}

	@SuppressWarnings("rawtypes")
	private boolean eligiblePos(BamPositionInformation posInfo){
		return !info[posInfo.distBetweenRefPosAndWinStart()] &&  
				posInfo.getChrInfo().getBinaryBase(posInfo.getRefPosition()) != posInfo.getBinaryBase();
	}
}
