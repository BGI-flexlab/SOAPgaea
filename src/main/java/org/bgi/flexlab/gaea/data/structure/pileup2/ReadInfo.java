package org.bgi.flexlab.gaea.data.structure.pileup2;

import org.bgi.flexlab.gaea.data.mapreduce.writable.ReadInfoWritable;
import org.bgi.flexlab.gaea.data.structure.alignment.SAMInfoBasic;
import org.bgi.flexlab.gaea.data.structure.bam.ParseSAMBasic;
import org.bgi.flexlab.gaea.data.structure.bam.ParseSAMInterface;
import org.bgi.flexlab.gaea.util.CigarState;

public class ReadInfo extends SAMInfoBasic implements ParseSAMInterface {
	private int end;
	private String sample;
	private int bestHitCount;
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(flag);
		sb.append("\t");
		sb.append(chrName);
		sb.append("\t");
		sb.append(position);
		sb.append("\t");
		sb.append(mappingQual);
		sb.append("\t");
		sb.append(cigarString);
		sb.append("\t");
		sb.append(readSequence);
		sb.append("\t");
		sb.append(qualityString);
		return sb.toString();
	}
	
	@Override
	public boolean parseSAM(String samline) {
		String[] alignmentArray = ParseSAMBasic.splitSAM(samline);
		flag = ParseSAMBasic.parseFlag(alignmentArray);
		if(isUnmapped())
			return false;
		chrName = ParseSAMBasic.parseChrName(alignmentArray);
		position = ParseSAMBasic.parsePosition(alignmentArray, true);
		if(position < 0)
			return false;
		mappingQual = ParseSAMBasic.parseMappingQuality(alignmentArray);
		cigarString = ParseSAMBasic.parseCigarString(alignmentArray);
		if(cigarString.equals("*"))
			return false;
		cigarState = new CigarState();
		cigarState.parseCigar(cigarString);
		int softClipStart = ParseSAMBasic.getStartSoftClipLength(cigarState.getCigar());
		int softClipEnd = ParseSAMBasic.getEndSoftClipLength(cigarState.getCigar());
		readSequence = ParseSAMBasic.parseSeq(alignmentArray, softClipStart, softClipEnd, false);
		qualityString = ParseSAMBasic.parseQual(alignmentArray, softClipStart, softClipEnd, false);
		end = ParseSAMBasic.parseCigar(position, cigarState)[0];
		sample = ParseSAMBasic.parseReadGroupID(alignmentArray);
		bestHitCount = ParseSAMBasic.parseBestHitCount(alignmentArray);
		return true;
	}

	@Override
	public boolean SAMFilter() {
		if(isUnmapped() || cigarString.equals("*") || readSequence.length() > qualityString.length() || position < 0) {
			return false;
		}
		if(isDup() || isQCFailed() || isSecondaryAlignment())
			return false;
		return true;
	}
	
	public boolean isUniqueRead() {
		return bestHitCount == 1;
	}
	
	public int getBestHitCount(){
		return bestHitCount;
	}

	public boolean parseAFC(ReadInfoWritable value) {
		if (value == null) {
			return false;
		}
		
		position = value.getPos().get();
		if(position < 0 ) {
			return false;
		}
		mappingQual = (short) value.getMAQ().get();
		qualityString = value.getQuality().toString();
		
		cigarState = new CigarState();
		cigarState.parseCigar(value.getCIGAR().toString());
		end = ParseSAMBasic.parseCigar(position, cigarState)[0];
		sample = String.valueOf(value.getSample());
		readSequence = value.getsequence(qualityString.length());
		return true;
	}
	
	public boolean parseDebug(String[] info){
		chrName = "chr1";
		position = Integer.parseInt(info[0]);
		mappingQual = Short.parseShort(info[1]);
		qualityString = info[2];
		cigarString = info[3];
		cigarState = new CigarState();
		cigarState.parseCigar(info[3]);
		end = ParseSAMBasic.parseCigar(position, cigarState)[0];
		sample = info[4];
		readSequence = info[5];
		flag = Integer.parseInt(info[6]);
		bestHitCount = Integer.parseInt(info[7]);
		return true;
	}
	
	/**
	 * @return the end
	 */
	public int getEnd() {
		return end;
	}

	/**
	 * @param end the end to set
	 */
	public void setEnd(int end) {
		this.end = end;
	}

	/**
	 * @return the rgID
	 */
	public String getSample() {
		return sample;
	}

	/**
	 * @param rgID the rgID to set
	 */
	public void setSample(String sample) {
		this.sample = sample;
	}
}

