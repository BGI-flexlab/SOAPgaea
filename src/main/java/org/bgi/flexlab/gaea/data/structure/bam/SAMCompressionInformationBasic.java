package org.bgi.flexlab.gaea.data.structure.bam;

import com.sun.tools.javac.util.ArrayUtils;
import htsjdk.samtools.CigarElement;
import htsjdk.samtools.CigarOperator;
import org.bgi.flexlab.gaea.data.exception.UserException;
import org.bgi.flexlab.gaea.data.structure.reads.ReadBasicCompressionInformation;
import org.bgi.flexlab.gaea.util.SAMUtils;
import org.bgi.flexlab.gaea.util.SystemConfiguration;

import java.util.Arrays;

public abstract class SAMCompressionInformationBasic extends ReadBasicCompressionInformation implements ParseSAMInterface{
	/**
	 * flag
	 */
	protected int flag = 0;
	
	/**
	 * chromosome name index
	 */
	protected int chrNameIndex = -1;

	/**
	 * alignment start
	 */
	protected int position = 0;
	
	/**
	 * mapping quality
	 */
	protected short mappingQual = 0;
	
	/**
	 * cigar in string
	 */
	protected int[] cigars;

	public SAMCompressionInformationBasic() {
		flag = 0;
		chrNameIndex = -1;
		position = 0;
		mappingQual = 0;
	}

	public SAMCompressionInformationBasic(SAMCompressionInformationBasic read) {
		super(read);
		this.flag = read.flag;
		this.chrNameIndex = read.chrNameIndex;
		this.position = read.position;
		this.mappingQual = read.mappingQual;
		this.cigars = Arrays.copyOf(read.cigars, read.cigars.length);
	}

	public boolean parseSAM(GaeaSamRecord samRecord) {

		flag = samRecord.getFlags();

		if(isUnmapped()) {
			return false;
		}

		chrNameIndex = samRecord.getReferenceIndex();

		position = samRecord.getAlignmentStart() - 1;

		if(position < 0) {
			return false;
		}

		mappingQual = (short) samRecord.getMappingQuality();


		if(samRecord.getCigarString().equals("*")) {
			return false;
		}

		int cigarLength = samRecord.getCigar().getCigarElements().size();
		cigars = new int[cigarLength];
		for(int i = 0; i < cigarLength; i++) {
			CigarElement cigar = samRecord.getCigar().getCigarElement(i);
			int cigarInt = (cigar.getLength() << 4) | CigarOperator.enumToBinary(cigar.getOperator());
			cigars[i] = cigarInt;
		}

		readBases = SAMUtils.bytesToCompressedBasesGaea(samRecord.getReadBases());

		qualities = samRecord.getBaseQualities();

		parseOtherInfo(samRecord);

		return true;
	}

	public boolean parseBamQC(String samline) {
		return true;
	}

	public abstract void parseOtherInfo(GaeaSamRecord samRecord);

	public boolean SAMFilter() {
		if(isUnmapped() || position < 0) {
			return false;
		}
		return true;
	}

	public int calculateReadEnd() {
		int end = position;
		for(int cigar : cigars) {
			int cigarOp = (cigar & SystemConfiguration.BAM_CIGAR_MASK);
			int cigarLength = cigar >> SystemConfiguration.BAM_CIGAR_SHIFT;
			if(cigarOp == SystemConfiguration.BAM_CMATCH || cigarOp == SystemConfiguration.BAM_CDEL || cigarOp == SystemConfiguration.BAM_CREF_SKIP || cigarOp == SystemConfiguration.BAM_CEQUAL) {
				end += cigarLength;
			}
		}
		//return end;
		return end - 1;
	}

	/**
	 * get alignment end with soft clip in count
	 * @return
	 */
	public int getSoftEnd() {
		int end = position;
		int cigar = cigars[cigars.length - 1];
		int cigarOp = (cigar & SystemConfiguration.BAM_CIGAR_MASK);

		if(cigarOp == SystemConfiguration.BAM_CSOFT_CLIP) {
			int cigarLength = cigar >> SystemConfiguration.BAM_CIGAR_SHIFT;
			end += cigarLength - 1;
		}

		return end;
	}

	/**
	 * get alignment end with soft clip in count
	 * @return
	 */
	public int getSoftStart() {
		int start = position;
		int cigar = cigars[0];
		int cigarOp = (cigar & SystemConfiguration.BAM_CIGAR_MASK);

		if(cigarOp == SystemConfiguration.BAM_CSOFT_CLIP) {
			int cigarLength = cigar >> SystemConfiguration.BAM_CIGAR_SHIFT;
			start -= cigarLength;
		}

		return start;
	}

	/**
	 * flag booleans
	 */
	public boolean hasMate() {
		return isQualified(SystemConfiguration.BAM_FPAIRED);
	}

	public boolean isPrpperPair() {
		return isQualified(SystemConfiguration.BAM_FPROPER_PAIR);
	}

	public boolean isUnmapped() {
		return isQualified(SystemConfiguration.BAM_FUNMAP);
	}

	public boolean isMateUnmapped() {
		return isQualified(SystemConfiguration.BAM_FMUNMAP);
	}

	public boolean isReverse() {
		return isQualified(SystemConfiguration.BAM_FREVERSE);
	}

	public boolean isMateReverse() {
		return isQualified(SystemConfiguration.BAM_FMREVERSE);
	}

	public boolean isFirstSegment() {
		return isQualified(SystemConfiguration.BAM_FREAD1);
	}

	public boolean isSecondaryAlignment() {
		return isQualified(SystemConfiguration.BAM_FSECONDARY);
	}

	public boolean isQCFailed() {
		return isQualified(SystemConfiguration.BAM_FQCFAIL);
	}

	public boolean isDup() {
		return isQualified(SystemConfiguration.BAM_FDUP);
	}

	private boolean isQualified(int config) {
		if((flag & config) != 0) {
			return true;
		}
		return false;
	}


	/**
	 * @return the flag
	 */
	public int getFlag() {
		return flag;
	}

	/**
	 * set flag value
	 * @param flag
	 */
	public void setFlag(int flag) {
		this.flag = flag;
	}

	/**
	 * 获取Read所在染色体的名称
	 * @return String
	 */
	public int getChrNameIndex() {
		return chrNameIndex;
	}


	/**
	 * set chromosome name index
	 * @param chrNameIndex
	 */
	public void setChrNameIndex(int chrNameIndex) {
		this.chrNameIndex = chrNameIndex;
	}

	/**
	 * 获取参考基因组上第一个碱基对的位置
	 * @return long
	 */
	public int getPosition() {
		return position;
	}


	/**
	 * set position
	 * @param position
	 */
	public void setPosition(int position) {
		this.position = position;
	}

	/**
	 * @return the mappingQual
	 */
	public short getMappingQual() {
		return mappingQual;
	}


	/**
	 * set mapping quality value
	 * @param mappingQual
	 */
	public void setMappingQual(short mappingQual) {
		this.mappingQual = mappingQual;
	}

	/**
	 * get cigars array
	 * @return
	 */
	public int[] getCigars() {
		return cigars;
	}

	/**
	 * set cigars
	 * @param cigars
	 */
	public void setCigars(int[] cigars) {
		this.cigars = cigars;
	}

	/**
	 * get cigar length
	 * @return
	 */
	public int getCigarsLength() {
		return cigars.length;
	}

}
