package org.bgi.flexlab.gaea.data.structure.pileup2;

public class PileupReadInfo {
	/**
	 * read information
	 */
	private ReadInfo readInfo;
	
	/**
	 * position on reads
	 */
	private int qpos;
	
	/**
	 * is deletionn base
	 */
	private boolean deletionBase = false;
	
	/**
	 * next is inert baes
	 */
	private boolean nextInsertBase = false;
	
	public PileupReadInfo(ReadInfo readInfo) {
		this.readInfo = readInfo;
	}

	public void calculateQposition(int refPos) {
		qpos = readInfo.getCigarState().resolveCigar(refPos, readInfo.getPosition());
		
		if(qpos == -1)
			deletionBase = true;
		else
			deletionBase = false;
		if(qpos == -2)
			nextInsertBase = true;
		else
			nextInsertBase = false;
	}
	
	
	/**
	 * @return the readInfo
	 */
	public ReadInfo getReadInfo() {
		return readInfo;
	}

	/**
	 * @param readInfo the readInfo to set
	 */
	public void setReadInfo(ReadInfo readInfo) {
		this.readInfo = readInfo;
	}

	/**
	 * @return the qpos
	 */
	public int getQpos() {
		return qpos;
	}
	
	public char getBase() {
		if(qpos < 0)
			return 0;
		return readInfo.getBaseFromRead(qpos);
	}
	
	public byte getByteBase() {
		return (byte)getBase();
	}
	
	public byte getBinaryBase() {
		if(qpos < 0)
			return (byte) qpos;
		return readInfo.getBinaryBase(qpos);
	}
	
	public char getBaseQuality() {
		if(qpos < 0)
			return 0;
		return readInfo.getBaseQuality(qpos);
	}
	
	public byte getByteBaseQuality() {
		return (byte)getBaseQuality();
	}

	public byte getBaseQualityValue(){
		if(qpos < 0)
			return (byte)qpos;
		return (byte)readInfo.getBaseQualityValue(qpos);
	}
	
	public int getMappingQuality() {
		return readInfo.getMappingQual();
	}

	/**
	 * @return the deletionBase
	 */
	public boolean isDeletionBase() {
		return deletionBase;
	}

	/**
	 * @return the nextInsertBase
	 */
	public boolean isNextInsertBase() {
		return nextInsertBase;
	}
	
	private int quality;
	private int base;
	
	public void setBaseQuality(int qual){
		this.quality = qual;
	}
	
	public void setBase(int b){
		this.base = b;
	}
}

