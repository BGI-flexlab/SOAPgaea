package org.bgi.flexlab.gaea.data.structure.region;

import org.bgi.flexlab.gaea.data.structure.region.SingleRegion.Regiondata;
import org.bgi.flexlab.gaea.report.util.ReportUtils;

public class RegionChromosomeInfo {
	private int size;
	private int coverageNum;
	private long depthNum;
	
	public RegionChromosomeInfo() {
		size = 0;
		coverageNum = 0;
		depthNum = 0;
	}
	
	public void add(Regiondata regiondata, BedSingleRegionStatistic singleRegionInfo) {
		size += regiondata.size();
		coverageNum += singleRegionInfo.getCoverageNum();
		depthNum += singleRegionInfo.getDepthNum();
	}
	
	public String toString(String chrName) {
		StringBuilder sb = new StringBuilder();
		sb.append(chrName);
		sb.append("\t");
		sb.append(size);
		sb.append("\t");
		sb.append(ReportUtils.doubleformat.format(getCoverage()));
		sb.append("\t");
		sb.append(ReportUtils.doubleformat.format(getAverageDepth()));
		sb.append("\n");
		
		return sb.toString();
	}

	/**
	 * @return the averageDepth
	 */
	public double getAverageDepth() {
		return depthNum / (double) coverageNum;
	}

	/**
	 * @return the coverage
	 */
	public double getCoverage() {
		return coverageNum / (double) size;
	}


	/**
	 * @return the size
	 */
	public int getSize() {
		return size;
	}
}
