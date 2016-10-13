package org.bgi.flexlab.gaea.data.structure.positioninformation;

import org.bgi.flexlab.gaea.data.structure.bam.SAMInformationBasic;

public interface CalculateWindowInformationInterface<T extends SAMInformationBasic> {
	public boolean add(CompoundInformation<T> winInfo);
}
