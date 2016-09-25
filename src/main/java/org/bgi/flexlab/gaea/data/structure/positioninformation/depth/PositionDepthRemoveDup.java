package org.bgi.flexlab.gaea.data.structure.positioninformation.depth;

import org.bgi.flexlab.gaea.data.structure.positioninformation.CalculatePositionInforamtionInterface;
import org.bgi.flexlab.gaea.data.structure.positioninformation.IntPositionInformation;
import org.bgi.flexlab.gaea.util.SAMInformationBasic;
import org.bgi.flexlab.gaea.data.structure.positioninformation.BamPositionInformation;

public class PositionDepthRemoveDup extends IntPositionInformation implements CalculatePositionInforamtionInterface<SAMInformationBasic>{

	public PositionDepthRemoveDup(int windowSize) {
		super(windowSize);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void add(BamPositionInformation<SAMInformationBasic> posInfo) {
		// TODO Auto-generated method stub
		if(!posInfo.isDup()) {
			info[posInfo.distBetweenRefPosAndWinStart()]++;
		}
	}
}
