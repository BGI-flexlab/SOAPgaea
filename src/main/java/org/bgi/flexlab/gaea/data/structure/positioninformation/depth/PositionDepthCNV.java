package org.bgi.flexlab.gaea.data.structure.positioninformation.depth;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.bgi.flexlab.gaea.data.structure.positioninformation.CalculatePositionInforamtionInterface;
import org.bgi.flexlab.gaea.data.structure.positioninformation.CompoundInformation;
import org.bgi.flexlab.gaea.util.SamRecordDatum;

public class PositionDepthCNV implements CalculatePositionInforamtionInterface<SamRecordDatum>{
	private ArrayList<PositionDepthCNVLane> cnvUsedDepth = new ArrayList<PositionDepthCNVLane>();
	private Map<Integer, Integer> rgIndex2depthIndex = new HashMap<Integer, Integer>();
	private int cnvDepthIndex = 0;
	private int laneSize;
	
	public PositionDepthCNV(int laneSize, int windowSize) {
		for(int i = 0; i < laneSize; i++) {
			PositionDepthCNVLane laneDepth = new PositionDepthCNVLane(windowSize);
			cnvUsedDepth.add(laneDepth);
		}
		this.laneSize = laneSize;
	}

	@Override
	public void add(CompoundInformation<SamRecordDatum> posInfo) {
		if(posInfo.eligiblePos() ) {
				//readInfo.getMappingQual() >= 10 && readInfo.getQualValue(readPosition) >= 15) {
			int depthIndex = 0;
			if(rgIndex2depthIndex.containsKey(posInfo.getRgIndex())) {
				depthIndex = rgIndex2depthIndex.get(posInfo.getRgIndex());
			} else {
				depthIndex = cnvDepthIndex;
				if(cnvDepthIndex >= laneSize){
					throw new RuntimeException("input data has more lane than BAM header!");
				}
				rgIndex2depthIndex.put(posInfo.getRgIndex(), cnvDepthIndex++);
			}
			PositionDepthCNVLane cnvLaneDepth = cnvUsedDepth.get(depthIndex);
			cnvLaneDepth.add(posInfo.distBetweenRefPosAndWinStart());
		}
	}
	
	public int getLaneSize() {
		return laneSize;
	}

	public PositionDepthCNVLane getLaneDepth(int index) {
		return cnvUsedDepth.get(index);
	}
}
