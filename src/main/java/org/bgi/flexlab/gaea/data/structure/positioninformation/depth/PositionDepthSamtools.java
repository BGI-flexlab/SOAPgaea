package org.bgi.flexlab.gaea.data.structure.positioninformation.depth;

import org.bgi.flexlab.gaea.data.structure.positioninformation.CalculatePositionInforamtionInterface;
import org.bgi.flexlab.gaea.data.structure.positioninformation.IntPositionInformation;
import org.bgi.flexlab.gaea.util.SAMInformationBasic;
import org.bgi.flexlab.gaea.data.structure.positioninformation.BamPositionInformation;

public class PositionDepthSamtools extends IntPositionInformation implements CalculatePositionInforamtionInterface<SAMInformationBasic>{

	public PositionDepthSamtools(int windowSize) {
		super(windowSize);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void add(BamPositionInformation posInfo) {
		if(posInfo.eligiblePos()) {
			info[posInfo.distBetweenRefPosAndWinStart()]++;
		}
	}

}
