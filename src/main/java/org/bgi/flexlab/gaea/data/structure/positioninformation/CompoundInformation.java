package org.bgi.flexlab.gaea.data.structure.positioninformation;

import org.bgi.flexlab.gaea.data.structure.reads.BamQCReadInformation;
import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformationShare;
import org.bgi.flexlab.gaea.util.CigarState;
import org.bgi.flexlab.gaea.util.SAMInformationBasic;
import org.bgi.flexlab.gaea.util.SystemConfiguration;

/**
 * a base class for compound information, which includes window information 
 * and position information (so far)
 * @param <T extends SAMInformationBasic>
 */
public class CompoundInformation<T extends SAMInformationBasic> {
	protected final T readInfo;
	
	protected final ChromosomeInformationShare chrInfo;
	
	protected final int windowStart;
	
	public CompoundInformation(int windowStart, T readInfo, ChromosomeInformationShare chrInfo) {
		this.windowStart = windowStart;
		this.readInfo = readInfo;
		this.chrInfo = chrInfo;
	}
	
	public boolean eligiblePos() {
		return (readInfo.getFlag() & (SystemConfiguration.BAM_FQCFAIL | 
				SystemConfiguration.BAM_FSECONDARY | SystemConfiguration.BAM_FDUP)) == 0 ;
	}

	public T getReadInfo() {
		return readInfo;
	}

	public int getRgIndex() {
		return ((BamQCReadInformation) readInfo).getRgIndex();
	}
	
	public int getMappingQual() {
		return readInfo.getMappingQual();
	}
	
	public CigarState getCigarState() {
		return readInfo.getCigarState();	
	}
	
	public boolean isDup() {
		return readInfo.isDup();
	}

	public ChromosomeInformationShare getChrInfo() {
		return chrInfo;
	}

	public int getWindowStart() {
		return windowStart;
	}

}
