package org.bgi.flexlab.gaea.data.structure.reads;

import org.apache.hadoop.hbase.util.Bytes;
import org.bgi.flexlab.gaea.util.BaseUtils;
import org.bgi.flexlab.gaea.util.SAMUtils;

public class ReadBasicCompressionInformation {
	protected byte[] readBases;
	protected byte[] qualities;
	protected static int MINIMUM_BASE_QUALITY = 33;

	public ReadBasicCompressionInformation() {
		this.readBases = null;
		this.qualities = null;
	}

	public ReadBasicCompressionInformation(byte[] readBases, byte[] qualities) {
		this.readBases = readBases;
		this.qualities = qualities;
	}
	
	public ReadBasicCompressionInformation(String readSequence, String qualityString) {
		this.readBases = SAMUtils.bytesToCompressedBasesGaea(readSequence.getBytes());
		this.qualities = qualityString.getBytes();
	}

	public void setMinimumBaseQuality(int minimumBaseQuality) {
		MINIMUM_BASE_QUALITY = minimumBaseQuality;
	}

	public int getReadLength() {
		return qualities.length;
	}

	public char getBaseFromRead(int position) {
		return (char) BaseUtils.baseIndexToSimpleBase(getBinaryBase(position));
	}

	/**
	 * get base index
	 * @param position start from 0
	 * @return
	 */
	public byte getBinaryBase(int position) {
		if(position % 2 != 0) {
			return (byte) (readBases[position / 2] >> 4);
		} else
			return (byte) (readBases[position / 2] & 0x0f);
	}

	/**
	 * get compressed read bases
	 * @return
	 */
	public byte[] getreadBases() {
		return readBases;
	}

	/**
	 * set read bases
	 * @param readBases
	 */
	public void setReadBases(byte[] readBases) {
		this.readBases = readBases;
	}

	/**
	 * get read seq in string
	 * @return
	 */
	public String getReadsSequence() {
		return Bytes.toString(getReadsOriginalSequenceBytes());
	}

	/**
	 * get read seq bytes
	 * @return
	 */
	public byte[] getReadsOriginalSequenceBytes() {
		return SAMUtils.compressedBasesToBytesGaea(qualities.length, readBases, 0);
	}

	/**
	 * get qualities bytes
	 * @return
	 */
	public byte[] getQualities() {
		return qualities;
	}

	/**
	 * set qualities bytes
	 * @param qualities
	 */
	public void setQualities(byte[] qualities) {
		this.qualities = qualities;
	}

	/**
	 * get base quality at position
	 * @param position
	 * @return
	 */
	public byte getBaseQuality(int position) {
		return qualities[position];
	}

	/**
	 * get base quality true value
	 * @param position
	 * @return
	 */
	public byte getBaseQualityValue(int position) {
		return (byte) (getBaseQuality(position) - MINIMUM_BASE_QUALITY);
	}
}
