package org.bgi.flexlab.gaea.data.structure.positioninformation;

import org.bgi.flexlab.gaea.util.SAMInformationBasic;

public interface CalculateWindowInformationInterface<T extends SAMInformationBasic> {
	public boolean add(WindowInformation<T>	winInfo);
}
