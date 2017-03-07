package org.bgi.flexlab.gaea.data.structure.region.statistic;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;

import org.bgi.flexlab.gaea.data.structure.region.SingleRegion.Regiondata;


public class CNVSingleRegionStatistic extends SingleRegionStatistic{
	private double middleDepth = 0;
	
	private ArrayList<Integer> depth = null;
	
	public CNVSingleRegionStatistic(boolean isPart) {
		if(isPart && depth == null)
			depth = new ArrayList<Integer>();
	}
	
	public String toString(Regiondata regiondata, boolean normalBedFormat, double allRegionAverageDepth) {
		double averageDepth = getDepthNum() /(double) regiondata.size();
		double coverage = coverBaseNum /(double) regiondata.size();
		double normalizedDepth = averageDepth / (double)allRegionAverageDepth;
		if(depth != null) {
			while(depth.size() < regiondata.size()) {
				depth.add(0);
			}
			Collections.sort(depth);
			middleDepth = calMiddleVaule(depth);
		}
		
		DecimalFormat df = new DecimalFormat("0.0000");
		df.setRoundingMode(RoundingMode.HALF_UP);
		StringBuilder outString = new StringBuilder();
		outString.append(regiondata.getNameString());
		outString.append("\t");
		outString.append(df.format(averageDepth));
		outString.append("\t");
		outString.append(df.format(middleDepth));
		outString.append("\t");
		outString.append(df.format(coverage*100));
		outString.append("\t");
		outString.append(df.format(normalizedDepth));
		outString.append("\n");
		
		return outString.toString();
	}
	
	public void add(int coverNum, int depthNum, double middleDepth) {
		this.coverBaseNum = coverNum;
		this.depthNum = depthNum;
		this.middleDepth = middleDepth;
	}
	
	public void addPart(int coverNum, int depthNum, ArrayList<Integer> deepthPart) {
		this.coverBaseNum += coverNum;
		this.depthNum += depthNum;

		for(int deep : deepthPart) {
			depth.add(deep);
		}
	}
	
	public double getDepth(Regiondata regiondata) {
		return getDepthNum() /(double) regiondata.size();
	}

	/**
	 * @return the deepthNum
	 */
	public long getDepthNum() {
		return depthNum;
	}
}
