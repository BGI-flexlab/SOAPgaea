package org.bgi.flexlab.gaea.util;

public class FastqInformationBasic {
	/**
	 * seq
	 */
	protected String seq;
	
	/**
	 * qual
	 */
	protected String qual;
	
	/**
	 * 
	 */
	public static int QUALBASE = 33;
	
	public FastqInformationBasic() {
		this.seq = "";
		this.qual = "";
	}
	
	/**
	 * constructor
	 * @param seq
	 * @param qual
	 */
	public FastqInformationBasic(String seq, String qual) {
		this.seq = seq;
		this.qual = qual;
	}
	
	public String getSeqString() {
		return seq;
	}
	
	public String getQualString() {
		return qual;
	}
	
	public byte[] getSeqBytes() {
		return seq.getBytes();
	}
	
	public byte[] getQualBytes() {
		return qual.getBytes();
	}
	
	public char getBase(int i) {
		return seq.charAt(i);
	}
	
	public byte getBinaryBase(int i) {
		return (byte) ((seq.charAt(i) >> 1) & 0x07);
	}
	
	public byte getBaseByte(int i) {
		return (byte) seq.charAt(i);
	}
	
	public char getQualChar(int i) {
		return qual.charAt(i);
	}
	
	public byte getQualByte(int i) {
		return (byte) qual.charAt(i);
	}
	
	public byte getQualValue(int i) {
		return (byte) (qual.charAt(i) - QUALBASE);
	}
}
