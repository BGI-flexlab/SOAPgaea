package org.bgi.flexlab.gaea.data.structure.reads;

import org.apache.hadoop.hbase.util.Bytes;
import org.bgi.flexlab.gaea.data.exception.UserException;
import org.bgi.flexlab.gaea.util.BaseUtils;
import org.bgi.flexlab.gaea.util.SAMUtils;

import java.util.Arrays;

public class ReadBasicCompressionInformation {
	protected byte[] readBases;
	protected byte[] qualities;
	protected static int MINIMUM_BASE_QUALITY = 33;

	public ReadBasicCompressionInformation() {
		this.readBases = null;
		this.qualities = null;
	}

	public ReadBasicCompressionInformation(ReadBasicCompressionInformation read) {
		readBases = Arrays.copyOf(read.readBases, read.readBases.length);
		qualities = Arrays.copyOf(read.qualities, read.qualities.length);
	}

	public ReadBasicCompressionInformation(byte[] readBases, byte[] qualities) {
		this.readBases = readBases;
		this.qualities = qualities;
	}
	
	public ReadBasicCompressionInformation(String readSequence, String qualityString) {
		this.readBases = SAMUtils.bytesToCompressedBasesGaea(readSequence.getBytes());
		this.qualities = qualityString.getBytes();
	}

	/**
	 * hard clip read
	 * @param toStartLength length to start
	 * @param toEndLength length to end
	 */
	public void hardClip(int toStartLength, int toEndLength) {
		if(toStartLength < 0 && toEndLength < 0)
			throw new UserException("both start and end < 0 for clip read");

		//bases
		if(toStartLength % 2 != 0) {
			byte[] readBases = new byte[(getReadLength() - toStartLength - toEndLength - 1) / 2 + 1];
			int i, j = 0;
			for(i = toStartLength / 2; i < readBases.length - (toEndLength / 2) - 1; i++) {
				readBases[j] = (byte) ((this.readBases[i] << 4) | (this.readBases[i + 1] & 0xf));
			}
			if(toEndLength % 2 == 0)
				readBases[j] = (byte) (this.readBases[i] << 4);
			this.readBases = readBases;
		} else {
			readBases = Arrays.copyOfRange(readBases, toStartLength / 2, readBases.length - (toEndLength / 2));
		}

		//qualities
		qualities = Arrays.copyOfRange(qualities, toStartLength, qualities.length - toEndLength);
	}

	/**
	 *  set minimum base quality
	 * @param minimumBaseQuality
	 */
	public void setMinimumBaseQuality(int minimumBaseQuality) {
		MINIMUM_BASE_QUALITY = minimumBaseQuality;
	}

	/**
	 * get read length
	 * @return
	 */
	public int getReadLength() {
		return qualities.length;
	}

	/**
	 * get base
	 * @param position
	 * @return
	 */
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
	public byte[] getReadBases() {
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
	 * get read region seq
	 * @param qStart
	 * @param length
	 * @return
	 */
	public String getReadBases(int qStart, int length) {
		byte[] bases = new byte[length];

		int i = 0;
		if(qStart % 2 != 0)
			bases[i++] = BaseUtils.baseIndexToSimpleBase((byte) (readBases[qStart / 2] >> 4));
		for(; i < length - 1; i += 2) {
			bases[i] = BaseUtils.baseIndexToSimpleBase((byte) (readBases[qStart / 2] & 0x0f));
			bases[i + 1] = BaseUtils.baseIndexToSimpleBase((byte) (readBases[qStart / 2] >> 4));
		}
		if(i - 2 < length - 1) {
			bases[length - 1] = BaseUtils.baseIndexToSimpleBase((byte) (readBases[qStart / 2] & 0x0f));
		}
		return String.valueOf(bases);
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

	public boolean isEmpty() {
		return readBases == null || qualities.length == 0;
	}

	public void emptyRead() {
		readBases = null;
		qualities = null;
	}
}
