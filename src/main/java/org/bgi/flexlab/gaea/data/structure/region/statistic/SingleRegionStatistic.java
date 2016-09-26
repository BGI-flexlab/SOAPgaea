package org.bgi.flexlab.gaea.data.structure.region.statistic;

import java.util.ArrayList;

public class SingleRegionStatistic {
	protected int coverBaseNum = 0;
	protected int depthNum = 0;
	
	public double calMiddleVaule(ArrayList<Integer> deepth) {
		double middleValue;
		if(deepth.size() == 0) {
			middleValue = 0;
			return middleValue;
		}
		if(deepth.size() % 2 == 0) {
			middleValue = (deepth.get(deepth.size() >> 1) + deepth.get((deepth.size() >> 1) - 1)) / (double)2;
		} else {
			middleValue = deepth.get(deepth.size() >> 1);
		}
		return middleValue;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(coverBaseNum);
		sb.append("\t");
		sb.append(depthNum);
		
		return sb.toString();
	}
	
	public void addCoverage() {
		coverBaseNum++;
	}
	
	public void addDepth(int depth) {
		depthNum += depth;
	}
}
