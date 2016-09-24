package org.bgi.flexlab.gaea.data.structure.region.report;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

import org.bgi.flexlab.gaea.data.structure.region.CNVSingleRegionStatistic;
import org.bgi.flexlab.gaea.data.structure.region.SingleRegion;
import org.bgi.flexlab.gaea.data.structure.region.SingleRegion.Regiondata;
import org.bgi.flexlab.gaea.data.structure.region.SingleRegionStatistic;
import org.bgi.flexlab.gaea.util.Utils;
import org.bgi.flexlab.gaea.util.posInfo.IntPositionInformation;

public class CNVSingleRegionReport extends SingleRegionReport<CNVSingleRegionStatistic>{
	private double allRegionAverageDeepth = 0;
	
	public CNVSingleRegionReport(SingleRegion singleReigon) {
		super(singleReigon);
		result = new ConcurrentHashMap<SingleRegion.Regiondata, CNVSingleRegionStatistic>();
	}
	
	public String getWholeRegionInfo(IntPositionInformation deep, int start, int end) {
		ArrayList<Integer> deepth = new ArrayList<Integer>();
		SingleRegionStatistic statistic = new SingleRegionStatistic();
		
		if(start > end) {
			throw new RuntimeException("start:" + start + "is bigger than end:" + end);
		}
		for(int i = start; i <= end; i++) {
			int deepNum = deep.get(i);
			if(deepNum > 0) {
				statistic.addCoverage();
				statistic.addDepth(deepNum);
			}
			deepth.add(deepNum);
		}
		Collections.sort(deepth);
		
		outputString.append(statistic.toString());
		outputString.append("\t");
		outputString.append(statistic.calMiddleVaule(deepth));
		outputString.append("\n");
		return outputString.toString();
	}
	
	public String getPartRegionInfo(IntPositionInformation deep, int start, int end) {
		SingleRegionStatistic statistic = new SingleRegionStatistic();
		
		if(deep.get(start) != 0){
			statistic.addCoverage();
			statistic.addDepth(deep.get(start));
		}
		outputString.append(deep.get(start));
		for(int i = start + 1; i <= end; i++) {
			if(deep.get(i) != 0){
				outputString.append("\t");
				outputString.append(deep.get(i));
				statistic.addCoverage();
				statistic.addDepth(deep.get(i));
			}
		}
		outputString.append(":");
		outputString.append(statistic.toString());
		outputString.append("\n");
		
		return outputString.toString();
	}
	
	public void parseReducerOutput(String line, boolean isPart) {
		int coverNum = 0;
		int depthNum = 0;
		String[] splits = line.split(":");

		if(isPart) {
			updatePartResult(splits, coverNum, depthNum);
		} else {
			updateResult(splits, coverNum, depthNum);
		}
	}
	
	private ArrayList<Integer> getDepth(String[] depthSplits){
		ArrayList<Integer> depth = new ArrayList<Integer>();
		for(String deep : depthSplits) {
			depth.add(Integer.parseInt(deep));
		}
		return depth;
	}
	
	private void updatePartResult(String[] splits, int coverNum, int depthNum) {
		ArrayList<Integer> depth = null;
		int regionIndex = Integer.parseInt(splits[0]);
		depth = getDepth(splits[1].split("\t"));
		String[] coverSplits = splits[2].split("\t");
		coverNum = Integer.parseInt(coverSplits[0]);
		depthNum = Integer.parseInt(coverSplits[1]);
		
		CNVSingleRegionStatistic statistic;
		if(!result.containsKey(singleReigon.getRegion(regionIndex))) {
			statistic = new CNVSingleRegionStatistic(true);
			statistic.addPart(coverNum, depthNum, depth);
			result.put(singleReigon.getRegion(regionIndex), statistic);
		} else {
			statistic = (CNVSingleRegionStatistic) result.get(singleReigon.getRegion(regionIndex));
			statistic.addPart(coverNum, depthNum, depth);
		}
	}
	
	private void updateResult(String[] splits, int coverNum, int depthNum) {
		int regionIndex = Integer.parseInt(splits[0]);
		String[] coverSplits = splits[1].split("\t");
		coverNum = Integer.parseInt(coverSplits[0]);
		depthNum =  Integer.parseInt(coverSplits[1]);
		double middleDeepth = Double.parseDouble(coverSplits[2]);
		
		CNVSingleRegionStatistic statistic = new CNVSingleRegionStatistic(false);
		statistic.add(coverNum, depthNum, middleDeepth);
		result.put(singleReigon.getRegion(regionIndex), statistic);
	}
	
	public void updateAllRegionAverageDeepth() {
		long deepthAll = 0;
		double regionSizeTotal = 0.0;
		updateResult();
		for(Regiondata regionData : result.keySet()) {
			CNVSingleRegionStatistic statistic = (CNVSingleRegionStatistic) result.get(regionData);
			String formatChrName = Utils.FormatChrName(regionData.getChrName());
			if(!formatChrName.equals("chrx") && !formatChrName.equals("chry") && !formatChrName.equals("chrm")) {
				deepthAll += statistic.getDepthNum();
				regionSizeTotal += regionData.size();
			}
		}
		System.err.println("deepthAll:" + deepthAll + "\nregionSizeTotal:" + regionSizeTotal);
		allRegionAverageDeepth = deepthAll / regionSizeTotal;
		System.err.println("allRegionAverageDeepth:" + allRegionAverageDeepth);
	}

	private void updateResult() {
		for(Regiondata regionData : singleReigon.getRegions()) {
			if(!result.containsKey(regionData)) {
				//System.err.println("no region:" + regionData.getNameString());
				CNVSingleRegionStatistic statistic = new CNVSingleRegionStatistic(false);
				result.put(regionData, statistic);
			}
		}
	}
	
	public double getAllRegionAverageDeepth() {
		return allRegionAverageDeepth;
	}
}
