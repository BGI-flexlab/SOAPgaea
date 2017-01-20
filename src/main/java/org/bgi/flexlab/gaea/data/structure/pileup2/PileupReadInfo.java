package org.bgi.flexlab.gaea.data.structure.pileup2;


import org.bgi.flexlab.gaea.data.structure.alignment.AlignmentsBasic;
import org.bgi.flexlab.gaea.data.structure.bam.SAMCompressionInformationBasic;
import org.bgi.flexlab.gaea.util.CigarState;

public class PileupReadInfo {
	/**
	 * read information
	 */
	protected AlignmentsBasic readInfo;

	/**
	 * position on reads
	 */
	protected int qpos;

	/**
	 * cigar state for cigar analysis
	 */
	protected CigarState cigarState = new CigarState();

	/**
	 * end for reads
	 */
	protected int end;

	/**
	 * sample info
	 */
	protected String sample;

	/**
	 * constructor
	 */
	public PileupReadInfo() {
		readInfo = null;
		qpos = Integer.MIN_VALUE;
		end = readInfo.getPosition();
		sample = null;
	}

	/**
	 * constructor
	 * @param readInfo
	 */
	public PileupReadInfo(AlignmentsBasic readInfo) {
		this.readInfo = readInfo;
		cigarState.parseCigar(readInfo.getCigars());
		end = readInfo.calculateReadEnd();
		sample = readInfo.getSample();
	}

	/**
	 * calculate query position
	 * @param refPos
	 */
	public void calculateQueryPosition(int refPos) {
		qpos = cigarState.resolveCigar(refPos, readInfo.getPosition());
	}
	
	
	/**
	 * @return the readInfo
	 */
	public SAMCompressionInformationBasic getReadInfo() {
		return readInfo;
	}

	/**
	 * @param readInfo the readInfo to set
	 */
	public void setReadInfo(AlignmentsBasic readInfo) {
		this.readInfo = readInfo;
	}

	/**
	 * @return the qpos
	 */
	public int getQpos() {
		return qpos;
	}

	/**
	 * return base in char
	 * @return
	 */
	public char getBase() {
		if(qpos < 0)
			return 0;
		return readInfo.getBaseFromRead(qpos);
	}

	/**
	 * return base in byte
	 * @return
	 */
	public byte getByteBase() {
		return (byte)getBase();
	}

	/**
	 * return binary 4bits base
	 * @return
	 */
	public byte getBinaryBase() {
		if(qpos < 0)
			return (byte) qpos;
		return readInfo.getBinaryBase(qpos);
	}

	/**
	 * return base quality
	 * @return
	 */
	public byte getBaseQuality() {
		if(qpos < 0)
			return -1;
		return readInfo.getBaseQuality(qpos);
	}

	/**
	 * return base quality in char
	 * @return
	 */
	public char getByteBaseQuality() {
		return (char)getBaseQuality();
	}

	/**
	 * return mapping quality
	 * @return
	 */
	public int getMappingQuality() {
		return readInfo.getMappingQual();
	}

	public int getPosition() {
		return readInfo.getPosition();
	}

	/**
	 * get end of alignment
	 * @return
	 */
	public int getEnd() {
		return end;
	}

	public String getSample() {
		return sample;
	}

	public boolean isDeletionBase() {
		return cigarState.isDeletionBase();
	}

	public boolean isNextDeletionBase() {
		return cigarState.isNextDeletionBase();
	}

	public boolean isNextInsertBase() {
		return cigarState.isNextInsertionBase();
	}

	public boolean isNextMatchBase() {
		return cigarState.isNextMatchBase();
	}
}

