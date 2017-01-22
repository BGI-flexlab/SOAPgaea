package org.bgi.flexlab.gaea.tools.bamqualtiycontrol.report;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;


public class BAMQCReport {
	/**
	 * basic information
	 */
	private BasicReport basicInfo;
	
	/**
	 * coverage information by chr
	 */
	private Map<String, CoverReport> coverInfo;
	
	/**
	 * region information
	 */
	private RegionReport regionInfo;
	
	/**
	 * single region information
	 */
	private CNVSingleRegionReport cnvSingleRegionInfo;
	
	/**
	 * bed single region information
	 */
	private BedSingleRegionReport bedSingleRegionInfo;
	
	/**
	 * gender single region information
	 */
	private BedSingleRegionReport genderSingleRegionInfo;
	
	/**
	 * region depth
	 */
	private RegionCoverReport regionCoverInfo;
	
	/**
	 * cnv depth
	 */
	private CNVDepthReport2 cnvDepthInfo;
	
	/**
	 * unmapped info
	 */
	private Map<String, ArrayList<Long>> unmappedSites = new ConcurrentHashMap<String, ArrayList<Long>>();
	
	/**
	 * insert size info
	 */
	private int[] insertSize = new int[2000];
	
	/**
	 * insert size info
	 */
	private int[] insertSizeWithoutDup = new int[2000];
	
	/**
	 * region or not?
	 */
	private boolean isRegion;
	
	private boolean cnvDepth = false;
	
	public BAMQCReport() {
		basicInfo = new BasicReport();
		coverInfo = new ConcurrentHashMap<String, CoverReport>();
		regionInfo = null;
		bedSingleRegionInfo = null;
		regionCoverInfo = new RegionCoverReport(1000);
		isRegion = false;
		cnvDepth = false;
		Arrays.fill(insertSize, 0);
		Arrays.fill(insertSizeWithoutDup, 0);
	}
	
	public BAMQCReport(Region region) {
		if(region == null) {
			basicInfo = new BasicReport();
			coverInfo = new ConcurrentHashMap<String, CoverReport>();
			regionInfo = null;
			bedSingleRegionInfo = null;
			regionCoverInfo = new RegionCoverReport(1000);
			isRegion = false;
		} else {
			basicInfo = new BasicReport();
			coverInfo = null;
			regionInfo = new RegionReport(region);
			bedSingleRegionInfo = null;
			regionCoverInfo = new RegionCoverReport(1000);
			isRegion = true;
		}
		Arrays.fill(insertSize, 0);
		Arrays.fill(insertSizeWithoutDup, 0);
	}
	
	public BAMQCReport(Region region, SingleRegion cnvSingleRegion, SingleRegion bedSingleRegion, SingleRegion genderSingleRegion, int laneDepthSize, boolean cnvDepth, SingleRegion cnvDepthRegion) {
		//System.err.println("gender region is null :" + (genderSingleRegion == null));
		//System.err.println("do cnv depth:" + cnvDepth + "\t lane size:" + laneDepthSize);
		//System.err.println("bed region is null :" + (bedSingleRegion == null));
		
		if(region == null) {
			basicInfo = new BasicReport();
			coverInfo = new ConcurrentHashMap<String, CoverReport>();
			regionInfo = null;
			isRegion = false;
			bedSingleRegionInfo = null;
			regionCoverInfo = new RegionCoverReport(1000);
			//System.err.println("region is null");
		} else {
			basicInfo = new BasicReport();
			coverInfo = null;
			regionInfo = new RegionReport(region);
			isRegion = true;
			regionCoverInfo = new RegionCoverReport(1000);
			if(cnvDepth && laneDepthSize > 0) {
				this.cnvDepth = true;
				cnvDepthInfo = new CNVDepthReport2(laneDepthSize, cnvDepthRegion);
			}
		}
		
		if(cnvSingleRegion != null) 
			cnvSingleRegionInfo = new CNVSingleRegionReport(cnvSingleRegion);
		else 
			cnvSingleRegionInfo = null;
		if(bedSingleRegion != null)
			bedSingleRegionInfo = new BedSingleRegionReport(bedSingleRegion);
		else
			bedSingleRegionInfo = null;
		if(genderSingleRegion != null)
			genderSingleRegionInfo = new BedSingleRegionReport(genderSingleRegion);
		else
			genderSingleRegionInfo = null;
		Arrays.fill(insertSize, 0);
		Arrays.fill(insertSizeWithoutDup, 0);
	}
	
	public String toReducerString(String sample, String chrName, boolean unmappedRegion) {
		StringBuffer info = new StringBuffer();
		
		if(chrName == "-1") {
			info.append("sample:");
			info.append(sample);
			info.append("\n");
			info.append(basicInfo.toReducerString());
			return info.toString();
		}
		info.append("sample:");
		info.append(sample);
		info.append("\n");
		info.append("chrName:");
		info.append(chrName);
		info.append("\n");
		info.append(basicInfo.toReducerString());
		if(!unmappedRegion) {
			info.append(regionCoverInfo.toReducerString());
			if(isRegion) {
				info.append(regionInfo.toReducerString());
				if(cnvDepth) {
					//System.err.println("do cnv depth");
					info.append(cnvDepthInfo.toReducerString());
				}
			} else {
				for(String key : coverInfo.keySet()) {
					CoverReport cover = coverInfo.get(key);
					info.append(cover.toReducerString());
				}
			}
			info.append("insert size information:\n");
			for(int i = 0; i < insertSize.length; i++) {
				if(insertSize[i] != 0) {
					info.append(i);
					info.append("\t");
					info.append(insertSize[i]);
					info.append("\n");
				}
			}
			info.append("insert size information\n");
			
			info.append("insert size without dup information:\n");
			for(int i = 0; i < insertSizeWithoutDup.length; i++) {
				if(insertSizeWithoutDup[i] != 0) {
					info.append(i);
					info.append("\t");
					info.append(insertSizeWithoutDup[i]);
					info.append("\n");
				}
			}
			info.append("insert size without dup information\n");
			
			info.append("unmapped site information:\n");
			for(String key : unmappedSites.keySet()) {
				ArrayList<Long> unmaped = unmappedSites.get(key);
				for(int i = 0; i < unmaped.size(); i += 2) {
					info.append(unmaped.get(i));
					info.append("\t");
					info.append(unmaped.get(i + 1));
					info.append("\n");
				}
			}
			info.append("unmapped site information\n");
			if(cnvSingleRegionInfo != null)
				info.append(cnvSingleRegionInfo.toReducerString());
			if(bedSingleRegionInfo != null) {
				//System.err.println("do bed region");
				info.append(bedSingleRegionInfo.toReducerString());
			}
			if(genderSingleRegionInfo != null) {
				//System.err.println("do gender region");
				info.append(genderSingleRegionInfo.toReducerString());
			}
		}
		return info.toString();
	}
	
	public boolean ParseCoverInfo(String line, GenomeShare genome) {
		String[] splitArray = line.split("\t");
		if(splitArray.length < 5){
			return false;
		}
		
		String chrName = splitArray[0];
		ChromosomeInfoShare chrInfo = genome.getChromosomeInfo(chrName);
		
		if(chrInfo == null) 
			return false;
		CoverReport cover;
		if(!coverInfo.containsKey(chrName)) {
			cover = new CoverReport(chrInfo);
			coverInfo.put(chrName, cover);
		} else {
			cover = coverInfo.get(chrName);
		}
		
		cover.coveredBaseNumIncrease(Long.parseLong(splitArray[1]));		
		cover.totalDeepthIncrease(Long.parseLong(splitArray[2]));
		cover.indelRefPosNumIncrease(Long.parseLong(splitArray[3]));
		cover.mismatchRefPosNumIncrease(Long.parseLong(splitArray[4]));
		
		return true;
	}
	
	public String toString() {
		StringBuffer info = new StringBuffer();
		if(isRegion) {
			info.append(basicInfo.toString());
			info.append(regionInfo.toString(basicInfo));
		} else {
			info.append(basicInfo.toString());
			
			info.append("coverage information:\n");
			TreeSet<String> keys = new TreeSet<String>(coverInfo.keySet());
			for(String key : keys) {
				CoverReport cover = coverInfo.get(key);
				info.append(cover.toString());
			}
		}
		
		return info.toString();
	}

	/**
	 * @return the basicInfo
	 */
	public BasicReport getBasicInfo() {
		return basicInfo;
	}

	/**
	 * @return the coverInfo
	 */
	public Map<String, CoverReport> getCoverInfo() {
		return coverInfo;
	}
	
	public void addChrCover(String chrName, ChromosomeInfoShare chrInfo) {
		CoverReport coverInfo = new CoverReport(chrName, chrInfo);
		this.coverInfo.put(chrName, coverInfo);
	}
	
	/**
	 * @return the regionInfo
	 */
	public RegionReport getRegionInfo() {
		return regionInfo;
	}
	
	public CNVSingleRegionReport getCNVSingleRegionInfo() {
		return cnvSingleRegionInfo;
	}
	
	public BedSingleRegionReport getBedSingleRegionInfo() {
		return bedSingleRegionInfo;
	}
	
	/**
	 * @return the genderSingleRegionInfo
	 */
	public BedSingleRegionReport getGenderSingleRegionInfo() {
		return genderSingleRegionInfo;
	}

	public RegionCoverReport getRegionCoverInfo() {
		return regionCoverInfo;
	}

	/**
	 * @return the cnvDepthInfo
	 */
	public CNVDepthReport2 getCnvDepthInfo() {
		return cnvDepthInfo;
	}

	/** 
	 * @return unmapped info
	 */
	public ArrayList<Long> getUnmappedSites(String chrName) {
		if(chrName == null || chrName == "") {
			return null;
		}
		
		if(!unmappedSites.containsKey(chrName)) {
			ArrayList<Long> sites = new ArrayList<Long>();
			unmappedSites.put(chrName, sites);
		}
		
		return unmappedSites.get(chrName);
	}
	
	/** 
	 * @return unmapped info
	 */
	public Map<String, ArrayList<Long>> getUnmappedSites() {
		return unmappedSites;
	}
	
	/**
	 * @return insert size info
	 */
	public int[] getInsertSize() {
		return insertSize;
	}
	
	/**
	 * @return insert size info
	 */
	public int[] getInsertSizeWithoutDup() {
		return insertSizeWithoutDup;
	}
	
	/**
	 * is region
	 * @return
	 */
	public boolean isRegion() {
		return isRegion;
	}

	/**
	 * @return the cnvDepth
	 */
	public boolean isCnvDepth() {
		return cnvDepth;
	}
}
