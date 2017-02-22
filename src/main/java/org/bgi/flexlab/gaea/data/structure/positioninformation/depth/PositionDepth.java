package org.bgi.flexlab.gaea.data.structure.positioninformation.depth;

import org.bgi.flexlab.gaea.data.structure.positioninformation.CalculateWindowInformationInterface;
import org.bgi.flexlab.gaea.data.structure.positioninformation.CompoundInformation;
import org.bgi.flexlab.gaea.data.structure.positioninformation.IntPositionInformation;
import org.bgi.flexlab.gaea.data.structure.positioninformation.depth.PositionDepthCNV;
import org.bgi.flexlab.gaea.data.structure.positioninformation.depth.PositionDepthGender;
import org.bgi.flexlab.gaea.data.structure.positioninformation.depth.PositionDepthNormal;
import org.bgi.flexlab.gaea.data.structure.positioninformation.depth.PositionDepthRemoveDuplication;
import org.bgi.flexlab.gaea.data.structure.positioninformation.other.PositionDeletionBaseInformation;
import org.bgi.flexlab.gaea.data.structure.positioninformation.other.PositionIndelInformation;
import org.bgi.flexlab.gaea.data.structure.positioninformation.other.PositionMismatchInformation;
import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformationShare;
import org.bgi.flexlab.gaea.util.SamRecordDatum;

public class PositionDepth implements CalculateWindowInformationInterface<SamRecordDatum>{
	
	private PositionDepthSamtools[] depths = null;

	private PositionDepthNormal posDepth = null;
	
	private PositionDepthRemoveDuplication posRMDupDepth = null;
	
	private PositionDepthCNV cnvUsedDepth = null;
	
	private PositionDepthGender genderUesdDepth = null;
	
	private PositionIndelInformation isIndel = null;
	
	private PositionMismatchInformation isMismatch = null;
	
	private PositionDeletionBaseInformation deletionBaseWithNOCover = null;
	
	public PositionDepth(int windowSize) {
		posDepth = new PositionDepthNormal(windowSize);
		isIndel = new PositionIndelInformation(windowSize);
		isMismatch = new PositionMismatchInformation(windowSize);
		deletionBaseWithNOCover = new PositionDeletionBaseInformation(windowSize);
	}
	
	public PositionDepth(int windowSize, boolean isGenderDepth, int laneSize) {
		this(windowSize);
//		if(isDupDepth)
		posRMDupDepth = new PositionDepthRemoveDuplication(windowSize);
		if(isGenderDepth) {
			genderUesdDepth = new PositionDepthGender(windowSize);
		}
		if(laneSize > 0) {
			cnvUsedDepth = new PositionDepthCNV(laneSize, windowSize);
		}
	}
	
	/**
	 * initialize index
	 * @return
	 */
	@Override
	public boolean add(CompoundInformation<SamRecordDatum> winInfo) {
		if(winInfo.getReadInfo() == null) {
			return false;
		}
		SamRecordDatum readInfo = winInfo.getReadInfo();
		ChromosomeInformationShare chrInfo = winInfo.getChrInfo();
		int winStart = winInfo.getWindowStart();
		
		for(int j = readInfo.getPosition(); j <= readInfo.getEnd(); j++) {
			if(j < winStart || j >= (winStart + winInfo.getWindowSize())) {
				continue;
			}
			int coord = readInfo.getCigarState().resolveCigar(j, readInfo.getPosition());
			
			CompoundInformation<SamRecordDatum> posInfo = new CompoundInformation<SamRecordDatum>(readInfo, chrInfo, winStart, j, coord);
			isIndel.add(posInfo);
			
			if( coord < 0) {//deletion
				if(deletionBaseWithNOCover != null) {
					deletionBaseWithNOCover.addDeleteBase(posDepth.get(j - winStart), j - winStart);
				}
				continue;
			}
			
			if(isMismatch != null) {
				isMismatch.add(posInfo);
			}
			
			if(posDepth != null) {
				posDepth.add(posInfo);
			}
			if(posRMDupDepth != null) {
				posRMDupDepth.add(posInfo);
			}
			if(genderUesdDepth != null) {
				genderUesdDepth.add(posInfo);
			}
			if(cnvUsedDepth != null) {
				cnvUsedDepth.add(posInfo);
			}
		}
		return true;
	}
	
	public int getPosDepth(int i) {
		return posDepth.get(i);
	}
	
	public IntPositionInformation getNormalPosDepth() {
		return posDepth;
	}
	
	public int getRMDupPosDepth(int i) {
		return posRMDupDepth.get(i);
	}
	
	public IntPositionInformation getRMDupPosDepth() {
		return posRMDupDepth;
	}

	public int getGenderUesdDepth(int pos) {
		return genderUesdDepth.get(pos);
	}
	
	public IntPositionInformation getGenderPosDepth() {
		return genderUesdDepth;
	}
	
	public int[] getLaneDepth(int i) {
		int[] laneDepth = new int[cnvUsedDepth.getLaneSize()];
		for(int j = 0; j < cnvUsedDepth.getLaneSize(); j++) {
			laneDepth[j] = cnvUsedDepth.getLaneDepth(j).get(i);
		}
		return laneDepth;
	}
	
	public PositionDepthCNV getPosDepthCNV() {
		return cnvUsedDepth;
	}
	
	public boolean hasIndelReads(int i) {
		return isIndel.get(i);
	}
	
	public boolean hasMismatchReads(int i) {
		return isMismatch.get(i);
	}
	
	public boolean isDeletionBaseWithNoConver(int i) {
		return deletionBaseWithNOCover.get(i);
	}
	
	public int getPosDepthSamtools(int sampleIndex, int i) {
		return depths[sampleIndex].get(i);
	}
	
	public IntPositionInformation getSamtoolsPosDepth(int sampleIndex) {
		return depths[sampleIndex];
	}
}
