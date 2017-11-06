/*******************************************************************************
 * Copyright (c) 2017, BGI-Shenzhen
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 *******************************************************************************/
package org.bgi.flexlab.gaea.data.structure.region.statistic;

import org.bgi.flexlab.gaea.data.structure.region.SingleRegion.Regiondata;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;


public class CNVSingleRegionStatistic extends SingleRegionStatistic{
	private double middleDepth = 0;

	private double middleRmdupDepth = 0;

	private ArrayList<Integer> depth = null;

	private ArrayList<Integer> rmdupDepth = null;

	public CNVSingleRegionStatistic(boolean isPart) {
		if(isPart && depth == null) {
			depth = new ArrayList<Integer>();
			rmdupDepth = new ArrayList<Integer>();
		}
	}
	
	public String toString(Regiondata regiondata, double allRegionAverageDepth) {
		double averageDepth = getDepth(regiondata);
		double averageRmdupDepth = getRmdupDepth(regiondata);
		double coverage = coverBaseNum /(double) regiondata.size();
		double normalizedDepth = averageDepth / (double)allRegionAverageDepth;
		if(depth != null) {
			while(depth.size() < regiondata.size()) {
				depth.add(0);
			}
			Collections.sort(depth);
			middleDepth = calMiddleVaule(depth);
		}
		if(rmdupDepth != null) {
			while(rmdupDepth.size() < regiondata.size()) {
				rmdupDepth.add(0);
			}
			Collections.sort(rmdupDepth);
			middleRmdupDepth = calMiddleVaule(rmdupDepth);
		}

		DecimalFormat df = new DecimalFormat("0.0000");
		df.setRoundingMode(RoundingMode.HALF_UP);
		StringBuilder outString = new StringBuilder();
		outString.append(regiondata.getChrName());
		outString.append("\t");
		outString.append(regiondata.getStart());
		outString.append("\t");
		outString.append(regiondata.getEnd());
		outString.append("\t");
		outString.append(df.format(averageDepth));
		outString.append("\t");
		outString.append(df.format(middleDepth));
		outString.append("\t");
		outString.append(df.format(averageRmdupDepth));
		outString.append("\t");
		outString.append(df.format(middleRmdupDepth));
		outString.append("\t");
		outString.append(df.format(coverage*100));
		outString.append("\t");
		outString.append(df.format(normalizedDepth));
		outString.append("\n");
		
		return outString.toString();
	}
	
	public void add(int coverNum, int depthNum, int rmdupDepthNum, double middleDepth, double middleRmdupDepth) {
		this.coverBaseNum = coverNum;
		this.depthNum = depthNum;
		this.rmdupDepthNum = rmdupDepthNum;
		this.middleDepth = middleDepth;
		this.middleRmdupDepth = middleRmdupDepth;
	}
	
	public void addPart(int coverNum, int depthNum, int rmdupDepthNum, ArrayList<String> depthPartList) {
		this.coverBaseNum += coverNum;
		this.depthNum += depthNum;
		this.rmdupDepthNum += rmdupDepthNum;

		// depthStr : depth,rmdupDepth
		for(String depthStr : depthPartList) {
			String[] depths = depthStr.split(",");
			depth.add(Integer.valueOf(depths[0]));
			rmdupDepth.add(Integer.valueOf(depths[1]));
		}
	}
	
	public double getDepth(Regiondata regiondata) {
		return getDepthNum() /(double) regiondata.size();
	}

	private double getRmdupDepth(Regiondata regiondata) {
		return getRmdupDepthNum() /(double) regiondata.size();
	}

}
