package org.bgi.flexlab.gaea.data.structure.positioninformation.depth;

import org.bgi.flexlab.gaea.data.structure.positioninformation.CalculatePositionInforamtionInterface;
import org.bgi.flexlab.gaea.data.structure.positioninformation.IntPositionInformation;
import org.bgi.flexlab.gaea.util.SAMInformationBasic;
import org.bgi.flexlab.gaea.data.structure.positioninformation.BamPositionInformation;

public class PositionDepthLimitMQ extends IntPositionInformation implements CalculatePositionInforamtionInterface<SAMInformationBasic>{
	private short minMQ = 0;
	
	public PositionDepthLimitMQ(int windowSize, short minMQ) {
		super(windowSize);
		this.minMQ = minMQ;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void add(BamPositionInformation posInfo) {
		if(posInfo.getMappingQual() >= minMQ)
			info[posInfo.distBetweenRefPosAndWinStart()]++;
	}
}
