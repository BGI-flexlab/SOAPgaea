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

import org.bgi.flexlab.gaea.data.exception.BAMQCException;
import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformationShare;
import org.bgi.flexlab.gaea.data.structure.region.SingleRegion.Regiondata;

public class BedSingleRegionStatistic extends SingleRegionStatistic{
	//private short[] ATCG;
	private double refGCrate;
		
	public BedSingleRegionStatistic() {
		depthNum = 0;
		coverBaseNum = 0;
		refGCrate = 0;
	}
	
	public BedSingleRegionStatistic(int depthNum, int coveragePosNum, short[] ATCG) {
		this.depthNum = depthNum;
		this.coverBaseNum = coveragePosNum;
		//this.ATCG = ATCG;
		refGCrate = 0;
	}
	
	public void add(String line) {
		String[] lineSplits = line.split("\t");
		if(lineSplits.length != 2) {
			throw new BAMQCException.WrongNumOfColException(2);
		}
		depthNum += Integer.parseInt(lineSplits[0]);
		coverBaseNum += Integer.parseInt(lineSplits[1]);
		/*for(int i = 0; i < 4; i++) {
			ATCG[i] += Short.parseShort(lineSplits[i + 2]);
		}*/
	}
	
	public void calRefGCrate(ChromosomeInformationShare chrInfo, Regiondata regionData, int extendSize) {
		int CGbaseNum = 0;
		int ATCGbaseNum = 0;
		if(extendSize == 0) {
			if(regionData.size() < 200) {
				extendSize = (200 - regionData.size()) / 2 + 1;
			}
		}
		for(char base : chrInfo.getBaseSequence(regionData.getStart() - extendSize > 0 ? regionData.getStart() - extendSize : 0, regionData.getEnd() + extendSize).toCharArray()) {
			if(base == 'C' || base == 'G' || base == 'c' || base == 'g') {
				CGbaseNum++;
			}
			if(base != 'N' && base != 'n') {
				ATCGbaseNum++;
			}
		}
		refGCrate = CGbaseNum / (double) ATCGbaseNum;
	}

	public int getCoverageNum() {
		return coverBaseNum; 
	}
	
	public double getAverageDepth() {
		if(coverBaseNum == 0) {
			return 0;
		}
		return depthNum / (double) coverBaseNum;
	}
	
	/*public double getGCRate() {
		return (ATCG[2] + ATCG[3]) / (double) (ATCG[0] + ATCG[1]);
	}*/

	public double getRefGCrate() {
		return refGCrate;
	}
}
