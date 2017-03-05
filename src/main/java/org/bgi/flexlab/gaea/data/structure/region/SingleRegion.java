package org.bgi.flexlab.gaea.data.structure.region;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.bgi.flexlab.gaea.util.FileIterator;

import htsjdk.tribble.readers.AsciiLineReader;
import htsjdk.tribble.readers.AsciiLineReaderIterator;


public class SingleRegion {
	private ArrayList<Regiondata> regions;
	Map<String, Integer[]> chrNameInterval;
	
	public SingleRegion() {
		regions = new ArrayList<SingleRegion.Regiondata>();
		chrNameInterval = new ConcurrentHashMap<String, Integer[]>();
	}
	
	public static void main(String[] args) throws IOException {
		SingleRegion singleRegion = new SingleRegion();
		singleRegion.parseRegionsFileFromHDFS("F:\\BGIBigData\\TestData\\Bed\\anno_bed_for_cnv_stat_PP600V3", false, 0);
		System.out.println(singleRegion.getRegion(1060).getStart());
		System.out.println(singleRegion.getRegion(1060).getEnd());
	}
	
	public void parseRegionsFileFromHDFS(String regionsFilePath, boolean normalBed, int extendSize) throws IOException {
		Regiondata lastRegionData = null;
		Integer[] chrInterval = new Integer[2];
		Arrays.fill(chrInterval, 0);
		
		FileIterator it = new FileIterator(regionsFilePath);
//		BufferedInputStream is = new BufferedInputStream(new FileInputStream(new File(regionsFilePath)));
//		AsciiLineReaderIterator it = new AsciiLineReaderIterator(new AsciiLineReader(is));
		while(it.hasNext()) {
			String line=it.next().toString().trim();
			if(line.equals("") || line == null || line.startsWith("#")) {
				continue;
			}
			Regiondata regionData = new Regiondata(line, normalBed);
			if(lastRegionData != null) {
				boolean nextRegion = updateRegion(regionData, lastRegionData, extendSize);
				if(nextRegion) {
					continue;
				}
			} else  {
				lastRegionData = initLastRegion(regionData, lastRegionData, extendSize);
			}
			
			chrInterval = updateChrInterval(lastRegionData, regionData, chrInterval);
			lastRegionData = initLastRegion(regionData, lastRegionData, extendSize);
		}
		regions.add(lastRegionData);
		chrNameInterval.put(lastRegionData.getChrName(), chrInterval);
		it.close();
	}
	
	private boolean canCombineRegion(Regiondata regionData, Regiondata lastRegionData, int extendSize) {
		return regionData.getStart() - lastRegionData.getEnd() < extendSize 
		&& lastRegionData.getChrName() == regionData.getChrName();
	}
	
	private Regiondata combineRegion(Regiondata regionData, Regiondata lastRegionData, int extendSize) {
		int start = lastRegionData.getStart();
		int end = lastRegionData.getEnd();
		return new Regiondata(regionData.getChrName(), start, regionData.getEnd() + extendSize > end ? 
																	regionData.getEnd() + extendSize : end);
	}
	
	private boolean extendRegion(Regiondata regionData, Regiondata lastRegionData, int extendSize){
		if(canCombineRegion(regionData, lastRegionData, extendSize)) {
			lastRegionData = combineRegion(regionData, lastRegionData, extendSize);
			return true;
		} else {
			regions.add(lastRegionData);
			//System.err.println("add:" + lastRegionData.getNameString());
		}
		return false;
	}
	
	private boolean updateRegion(Regiondata regionData, Regiondata lastRegionData, int extendSize) {
		boolean nextRegion = false;
		if(extendSize > 0) {
			nextRegion = extendRegion(regionData, lastRegionData, extendSize);
		} else {
			if(lastRegionData.getChrName().equals(regionData.chrName) && regionData.getStart() < lastRegionData.getStart()) {
				throw new RuntimeException("anno region is not sorted:" + regionData.getStart() + " < " + lastRegionData.getStart());
			}
			regions.add(lastRegionData);
		}
		return nextRegion;
	}
	
	private Regiondata initLastRegion(Regiondata regionData, Regiondata lastRegionData, int extendSize) {
		if(extendSize > 0) {
			lastRegionData = new Regiondata(regionData.getChrName(), regionData.getStart() - extendSize > 0 ? regionData.getStart() - extendSize : 0, regionData.getEnd() + extendSize);
		} else {
			lastRegionData = regionData;
		}
		return lastRegionData;
	}
	
	private void updateChrNameInterval(Integer[] chrInterval, Regiondata lastRegionData) {
		Integer[] tmp = new Integer[2];
		tmp[0] = chrInterval[0];
		tmp[1] = chrInterval[1];
		chrNameInterval.put(lastRegionData.getChrName(), tmp);
	}
	
	private Integer[] updateChrInterval(Regiondata lastRegionData, Regiondata regionData, Integer[] chrInterval) { 
		if(lastRegionData.getChrName().equals(regionData.chrName)) {
			chrInterval[1]++;
		} else {
			updateChrNameInterval(chrInterval, lastRegionData);
			chrInterval[0] = chrInterval[1]++;
		}
		
		return chrInterval;
	}
	
	public int posInRegion(String chrName, int pos) {
		Integer[] chrInterval = chrNameInterval.get(chrName);
		if(chrInterval == null || pos < 0) {
			return -1;
		}
		
		int leftIndex = chrInterval[0];
		int rightIndex = chrInterval[1] - 1;
		
		while(leftIndex <= rightIndex) {
			int middleIndex = ((leftIndex + rightIndex) >> 1);
			Regiondata regionData = regions.get(middleIndex);
			if(pos >= regionData.start && pos <= regionData.end) {
				return getFirstRegion(middleIndex, pos);
			} else if(pos < regionData.start) {
				rightIndex = middleIndex - 1;
			} else if(pos > regionData.end) {
				leftIndex = middleIndex + 1;
			}
		}
		
		return -1;
	}
	
	private int getFirstRegion(int index, int pos) {
		int finalIndex = index;
		String chrName = "";
		for(int i = index; i >= 0; i--) {
			Regiondata regionData = regions.get(i);
			if(pos < regionData.start || pos > regionData.end) {
				if(chrName != "" && chrName != regionData.getChrName()) {
					break;
				}
				finalIndex = i + 1;
			}
			if(pos > regionData.end) {
				break;
			}
			chrName = regionData.getChrName();
		}
		return finalIndex;
	}
	
	public Regiondata getRegion(int i) {
		return regions.get(i);
	}
	
	public Integer[] getChrInterval(String chrName) {
		return chrNameInterval.get(chrName);
	}

	public ArrayList<Regiondata> getRegions() {
		return regions;
	}
	
	public class Regiondata {
		private boolean bedFormat = false;
		private String Name1;
		private String Name2;
		private String chrName;
		private int start;
		private int end;
		
		public Regiondata(String Name1, String Name2, String chrName, int start, int end) {
			this.Name1 = Name1;
			this.Name2 = Name2;
			init(chrName, start, end);
		}
		
		public Regiondata(String chrName, int start, int end) {
			bedFormat = true;
			init(chrName, start, end);
		}
		
		public Regiondata(String regionLine, boolean normalBedFormat) {
			String[] lineSplits = regionLine.split("\t");
			int index = 0;
			bedFormat = normalBedFormat;
			if(!normalBedFormat) {
				this.Name1 = lineSplits[index++];
				this.Name2 = lineSplits[index++];
			}
			init(lineSplits[index++], Integer.parseInt(lineSplits[index++]),
					Integer.parseInt(lineSplits[index++]) - 1);
		}
		
		public void init(String chrName, int start, int end) {
			this.chrName = chrName;
			this.start = start;
			this.end = end;
		}
		
		public void combine(Regiondata data) {
			if(chrName != data.getChrName())
				throw new RuntimeException("chr name of 2 region data is different.");
			if(data.getStart() < start) {
				start = data.getStart();
			}
			if(data.getEnd() > end) {
				end = data.getEnd();
			}
		}
		
		public String getNameString() {
			StringBuilder nameString = new StringBuilder();
			if(!bedFormat) {
				nameString.append(Name1);
				nameString.append("\t");
				nameString.append(Name2);
			} else {
				nameString.append(chrName);
				nameString.append("\t");
				nameString.append(start);
				nameString.append("\t");
				nameString.append(end + 1);
			}
			return nameString.toString();
		}
		
		public int size() {
			return end -start + 1;
		}

		/**
		 * @return the name1
		 */
		public String getName1() {
			return Name1;
		}

		/**
		 * @param name1 the name1 to set
		 */
		public void setName1(String name1) {
			Name1 = name1;
		}

		/**
		 * @return the name2
		 */
		public String getName2() {
			return Name2;
		}

		/**
		 * @param name2 the name2 to set
		 */
		public void setName2(String name2) {
			Name2 = name2;
		}

		/**
		 * @return the chrName
		 */
		public String getChrName() {
			return chrName;
		}

		/**
		 * @param chrName the chrName to set
		 */
		public void setChrName(String chrName) {
			this.chrName = chrName;
		}

		/**
		 * @return the start
		 */
		public int getStart() {
			return start;
		}

		/**
		 * @param start the start to set
		 */
		public void setStart(int start) {
			this.start = start;
		}

		/**
		 * @return the end
		 */
		public int getEnd() {
			return end;
		}

		/**
		 * @param end the end to set
		 */
		public void setEnd(int end) {
			this.end = end;
		}
	}

}
