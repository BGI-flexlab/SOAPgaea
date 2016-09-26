package org.bgi.flexlab.gaea.data.structure.positioninformation.other;

import org.bgi.flexlab.gaea.data.structure.positioninformation.BooleanPositionInformation;
import org.bgi.flexlab.gaea.data.structure.positioninformation.CalculatePositionInforamtionInterface;
import org.bgi.flexlab.gaea.data.structure.positioninformation.CompoundInformation;
import org.bgi.flexlab.gaea.util.SAMInformationBasic;

public class PositionMismatchInformation extends BooleanPositionInformation implements CalculatePositionInforamtionInterface<SAMInformationBasic>{

	public PositionMismatchInformation(int windowSize) {
		super(windowSize);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void add(CompoundInformation posInfo) {
		if(eligiblePos(posInfo)) {
			info[posInfo.distBetweenRefPosAndWinStart()] = true;
		}
	}

	@SuppressWarnings("rawtypes")
	private boolean eligiblePos(CompoundInformation posInfo){
		return !info[posInfo.distBetweenRefPosAndWinStart()] &&  
				posInfo.getChrInfo().getBinaryBase(posInfo.getRefPosition()) != posInfo.getBinaryBase();
	}
}
