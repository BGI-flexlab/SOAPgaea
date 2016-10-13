package org.bgi.flexlab.gaea.data.structure.positioninformation;

import org.bgi.flexlab.gaea.data.structure.bam.SAMInformationBasic;

public interface CalculatePositionInforamtionInterface<T extends SAMInformationBasic> {
	public void add(CompoundInformation<T> posInfo);
}
