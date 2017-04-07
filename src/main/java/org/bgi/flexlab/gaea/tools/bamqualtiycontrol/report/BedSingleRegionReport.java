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
package org.bgi.flexlab.gaea.tools.bamqualtiycontrol.report;

import java.util.concurrent.ConcurrentHashMap;
import org.bgi.flexlab.gaea.data.structure.positioninformation.IntPositionInformation;
import org.bgi.flexlab.gaea.data.structure.region.SingleRegion;
import org.bgi.flexlab.gaea.data.structure.region.statistic.BedSingleRegionStatistic;

public class BedSingleRegionReport extends
		SingleRegionReport<BedSingleRegionStatistic> {
	public BedSingleRegionReport(SingleRegion singleReigon) {
		super(singleReigon);
		result = new ConcurrentHashMap<SingleRegion.Regiondata, BedSingleRegionStatistic>();
	}

	@Override
	public String getWholeRegionInfo(IntPositionInformation deep, int start,
			int end) {
		int coverageNum = 0;
		int depthNum = 0;

		for (int i = start; i <= end; i++) {
			int deepNum = deep.get(i);
			if (deepNum > 0) {
				coverageNum++;
				depthNum += deepNum;
			}
		}

		outputString.append(depthNum);
		outputString.append("\t");
		outputString.append(coverageNum);
		outputString.append("\n");

		return outputString.toString();
	}

	@Override
	public String getPartRegionInfo(IntPositionInformation deep, int start,
			int end) {
		return getWholeRegionInfo(deep, start, end);
	}

	@Override
	public void parseReducerOutput(String line, boolean isPart) {
		String[] splits = line.split(":");
		int regionIndex = Integer.parseInt(splits[0]);
		BedSingleRegionStatistic statistic = null;
		if (isPart) {
			updatePartResult(statistic, splits, regionIndex);
		} else {
			updateResult(statistic, splits, regionIndex);
		}
	}

	private void updateResult(BedSingleRegionStatistic statistic,
			String[] splits, int regionIndex) {
		statistic = new BedSingleRegionStatistic();
		statistic.add(splits[1]);
		result.put(singleReigon.getRegion(regionIndex), statistic);
	}

	private void updatePartResult(BedSingleRegionStatistic statistic,
			String[] splits, int regionIndex) {
		if (!result.containsKey(singleReigon.getRegion(regionIndex))) {
			updateResult(statistic, splits, regionIndex);
		} else {
			statistic = result.get(singleReigon.getRegion(regionIndex));
			statistic.add(splits[1]);
		}
	}
}