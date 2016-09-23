package org.bgi.flexlab.gaea.data.structure.region;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;

import org.bgi.flexlab.gaea.data.structure.region.SingleRegion.Regiondata;


public class CNVSingleRegionStatistic extends SingleRegionStatistic{
	private double middleDeepth = 0;
	
	private ArrayList<Integer> deepth = null;
	
	public CNVSingleRegionStatistic(boolean isPart) {
		if(isPart && deepth == null)
			deepth = new ArrayList<Integer>();
	}
	
	public String toString(Regiondata regiondata, boolean normalBedFormat, double allRegionAverageDeepth) {
		double averageDeepth = getDepthNum() /(double) regiondata.size();
		double coverage = coverBaseNum /(double) regiondata.size();
		double NormalizedDeepth = averageDeepth / (double)allRegionAverageDeepth;
		if(deepth != null) {
			while(deepth.size() < regiondata.size()) {
				deepth.add(0);
			}
			Collections.sort(deepth);
			middleDeepth = calMiddleVaule(deepth);
		}
		
		DecimalFormat df = new DecimalFormat("0.0000");
		df.setRoundingMode(RoundingMode.HALF_UP);
		StringBuilder outString = new StringBuilder();
		outString.append(regiondata.getNameString());
		outString.append("\t");
		outString.append(df.format(averageDeepth));
		outString.append("\t");
		outString.append(df.format(middleDeepth));
		outString.append("\t");
		outString.append(df.format(coverage*100));
		outString.append("\t");
		outString.append(df.format(NormalizedDeepth));
		outString.append("\n");
		
		return outString.toString();
	}
	
	public void add(int coverNum, int depthNum, double middleDeepth) {
		this.coverBaseNum = coverNum;
		this.depthNum = depthNum;
		this.middleDeepth = middleDeepth;
	}
	
	public void addPart(int coverNum, int depthNum, ArrayList<Integer> deepthPart) {
		this.coverBaseNum += coverNum;
		this.depthNum += depthNum;

		for(int deep : deepthPart) {
			deepth.add(deep);
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
