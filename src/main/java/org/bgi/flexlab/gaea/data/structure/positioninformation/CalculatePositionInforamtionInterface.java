package org.bgi.flexlab.gaea.data.structure.positioninformation;

import org.bgi.flexlab.gaea.util.SAMInformationBasic;

public interface CalculatePositionInforamtionInterface<T extends SAMInformationBasic> {
	public void add(CompoundInformation<T> posInfo);
}
