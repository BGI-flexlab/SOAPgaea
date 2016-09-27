package org.bgi.flexlab.gaea.data.structure.positioninformation.depth;

import org.bgi.flexlab.gaea.data.structure.positioninformation.CalculatePositionInforamtionInterface;
import org.bgi.flexlab.gaea.data.structure.positioninformation.CompoundInformation;
import org.bgi.flexlab.gaea.data.structure.positioninformation.IntPositionInformation;
import org.bgi.flexlab.gaea.util.SAMInformationBasic;

public class PositionDepthSamtools extends IntPositionInformation implements CalculatePositionInforamtionInterface<SAMInformationBasic>{

	public PositionDepthSamtools(int windowSize) {
		super(windowSize);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void add(CompoundInformation posInfo) {
		if(posInfo.eligiblePos()) {
			info[posInfo.distBetweenRefPosAndWinStart()]++;
		}
	}

}
