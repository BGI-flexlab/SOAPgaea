package org.bgi.flexlab.gaea.data.structure.alignment;

import org.bgi.flexlab.gaea.data.structure.reads.ReadInformation;
import org.bgi.flexlab.gaea.util.CigarState;
import org.bgi.flexlab.gaea.util.SystemConfiguration;

public class SAMInfoBasic extends ReadInformation {
	/**
	 * flag
	 */
	protected int flag;
	
	/**
	 * 染色体名称
	 */
	protected String chrName;

	/**
	 * 参考基因组上第一个碱基对的坐标值
	 */
	protected int position;
	
	/**
	 * mapping quality
	 */
	protected short mappingQual = 0;
	
	/**
	 * cigar in string
	 */
	protected String cigarString;
	
	/**
	 * cigar state
	 */
	protected CigarState cigarState;

	/**
	 * flag booleans
	 */
	public boolean hasMate() {
		if((flag & SystemConfiguration.BAM_FPAIRED) != 0)
			return true;
		return false;
	}
	
	public boolean isPrpperPair() {
		if((flag & SystemConfiguration.BAM_FPROPER_PAIR) != 0)
			return true;
		return false;
	}
	
	public boolean isUnmapped() {
		if((flag & SystemConfiguration.BAM_FUNMAP) != 0)
			return true;
		return false;
	}
	
	public boolean isMateUnmapped() {
		if((flag & SystemConfiguration.BAM_FMUNMAP) != 0)
			return true;
		return false;
	}
	
	public boolean isReverse() {
		if((flag & SystemConfiguration.BAM_FREVERSE) != 0)
			return true;
		return false;
	}
	
	public boolean isMateReverse() {
		if((flag & SystemConfiguration.BAM_FMREVERSE) != 0)
			return true;
		return false;
	}
	
	public boolean isFirstSegment() {
		if((flag & SystemConfiguration.BAM_FREAD1) != 0)
			return true;
		return false;
	}
	
	public boolean isSecondaryAlignment() {
		if((flag & SystemConfiguration.BAM_FSECONDARY) != 0)
			return true;
		return false;
	}
	
	public boolean isQCFailed() {
		if((flag & SystemConfiguration.BAM_FQCFAIL) != 0)
			return true;
		return false;
	}
	
	public boolean isDup() {
		if((flag & SystemConfiguration.BAM_FDUP) != 0)
			return true;
		return false;
	}
	

	/**
	 * 获取参考基因组上第一个碱基对的位置
	 * @return long
	 */
	public int getPosition() {
		return position;
	}

	/**
	 * 获取Read所在染色体的名称
	 * @return String
	 */
	public String getChrName() {
		return chrName;
	}

	public String getCigarString() {
		return cigarString;
	}

	/**
	 * @return the flag
	 */
	public int getFlag() {
		return flag;
	}

	/**
	 * @return the mappingQual
	 */
	public short getMappingQual() {
		return mappingQual;
	}
	
	public CigarState getCigarState() {
		return cigarState;
	}
}
