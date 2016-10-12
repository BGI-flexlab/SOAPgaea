package org.bgi.flexlab.gaea.data.structure.positioninformation.other;

import org.bgi.flexlab.gaea.data.structure.bam.SAMInformationBasic;
import org.bgi.flexlab.gaea.data.structure.positioninformation.CalculatePositionInforamtionInterface;
import org.bgi.flexlab.gaea.data.structure.positioninformation.CompoundInformation;
import org.bgi.flexlab.gaea.data.structure.positioninformation.PositionInformationUtils;

public class PositionATCGInformation extends PositionInformationUtils<ATCGCount> implements CalculatePositionInforamtionInterface<SAMInformationBasic>{
	
	public PositionATCGInformation(int windowSize) {
		super(windowSize);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void add(CompoundInformation posInfo) {
		// TODO Auto-generated method stub
		int base = posInfo.getBinaryBase();
		if(base < 4) {
			info.get(posInfo.distBetweenRefPosAndWinStart()).add(1, base);
		}
	}
	
	
}
