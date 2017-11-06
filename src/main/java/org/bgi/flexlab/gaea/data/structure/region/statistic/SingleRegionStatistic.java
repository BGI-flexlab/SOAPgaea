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

import java.util.ArrayList;

public class SingleRegionStatistic {
	protected int coverBaseNum = 0;
	protected int depthNum = 0;
	protected int rmdupDepthNum = 0;

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
		sb.append("\t");
		sb.append(rmdupDepthNum);
		
		return sb.toString();
	}
	
	public void addCoverage() {
		coverBaseNum++;
	}
	
	public void addDepth(int depth) {
		depthNum += depth;
	}

	public void addRmdupDepth(int rmdupDepth) {
		rmdupDepthNum += rmdupDepth;
	}

	/**
	 * @return the depthNum
	 */
	public long getDepthNum() {
		return depthNum;
	}

	public long getRmdupDepthNum() {
		return rmdupDepthNum;
	}

	public int getCoverBaseNum() {
		return coverBaseNum;
	}

}
