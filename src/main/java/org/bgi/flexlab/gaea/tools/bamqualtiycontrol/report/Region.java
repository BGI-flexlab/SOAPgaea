package org.bgi.flexlab.gaea.tools.bamqualtiycontrol.report;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.bgi.flexlab.gaea.data.mapreduce.input.header.SamHdfsFileHeader;
import org.bgi.flexlab.gaea.data.structure.positioninformation.depth.PositionDepth;
import org.bgi.flexlab.gaea.data.structure.reference.ReferenceShare;
import org.bgi.flexlab.gaea.data.structure.region.SingleRegion;
import org.bgi.flexlab.gaea.data.structure.region.TargetRegion;
import org.bgi.flexlab.gaea.data.structure.region.report.BedSingleRegionReport;
import org.bgi.flexlab.gaea.tools.bamqualtiycontrol.SamRecordDatum;
import org.bgi.flexlab.gaea.tools.mapreduce.bamqualitycontrol.BamQualityControlOptions;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMReadGroupRecord;

public class Region extends OutputType{
	
	private TargetRegion region;
	
	private SingleRegion cnvRegion;
	
	private RegionReport regionReport;
	
	private BedSingleRegionReport bedSingleRegionReport;
	
	private BedSingleRegionReport genderSingleRegionReport;
	
	private CNVDepthReport cnvDepthReport;
	
	private Map<String, Integer> sampleLaneSize = new HashMap<>();
					
	public Region(BamQualityControlOptions options, Configuration conf) throws IOException {
		super(options);
		region = new TargetRegion();
		cnvRegion = new SingleRegion();
		if (options.getRegion() != null) {
			region.parseRegion(options.getRegion(), true);
			regionReport = new RegionReport(region);
		}
		if (options.getBedfile() != null) {
			region.parseBedFileFromHDFS(options.getBedfile(), true);
			regionReport = new RegionReport(region);
			
			SingleRegion bedRegion = new SingleRegion();
			bedRegion.parseRegionsFileFromHDFS(options.getBedfile(), true, 0);
			bedSingleRegionReport = new BedSingleRegionReport(bedRegion);
			
			SingleRegion genderRegion = new SingleRegion();
			genderRegion.parseRegionsFileFromHDFS(options.getBedfile(), true, 0);
			genderSingleRegionReport = new BedSingleRegionReport(genderRegion);
		}
		
		if(options.isCnvDepth())
			initSampleLaneSize(conf);
	}
	
	public void initCNVDepthReport(String sample) throws IOException {
		if(options.isCnvDepth() && options.getCnvRegion() != null) {
			cnvRegion = new SingleRegion();
			cnvRegion.parseRegionsFileFromHDFS(options.getCnvRegion(), true, 0);
			cnvDepthReport = new CNVDepthReport(sampleLaneSize.get(sample),cnvRegion);
		}
	}
	
	private void initSampleLaneSize(Configuration conf) {
		SAMFileHeader mFileHeader = SamHdfsFileHeader.getHeader(conf);
		for(SAMReadGroupRecord rg : mFileHeader.getReadGroups()) {
			if(sampleLaneSize.containsKey(rg.getSample())) {
				int tmp = sampleLaneSize.get(rg.getSample());
				sampleLaneSize.put(rg.getSample(), tmp + 1);
			} else {
				sampleLaneSize.put(rg.getSample(), 1);
			}
		}
	}
	
	@Override
	public boolean mappedReport(SamRecordDatum datum, ReferenceShare genome, String chrName, Context context) {
		super.mappedReport(datum, genome, chrName, context);
		regionReport.constructMapReport(chrName, datum);
		return true;
	}
	
	@Override
	public void depthReport(PositionDepth pd, int i, String chrName, long pos) {
		int depth = pd.getPosDepth(i);
		int noPCRdepth = pd.getRMDupPosDepth(i);
		if(region.isPositionInRegion(chrName, pos)) {
			super.regionCoverReport(depth, noPCRdepth);
			if(options.isOutputUnmapped() && depth != 0 )
				unmappedReport.updateUnmappedSites(pos, unmappedReport.getUnmappedSites(chrName));
		}
		if(options.isCnvDepth() && cnvRegion.posInRegion(chrName, (int) (pos)) != -1) {
			cnvDepthReport.add(chrName, (int)pos, pd.getLaneDepth(i));
		}
		
		regionReport.constructDepthReport(pd, i, chrName, pos);
	}
	
	@Override 
	public void singleRegionReports(String chrName, long winStart, int winSize , PositionDepth pd) {
		super.singleRegionReports(chrName, winStart, winSize, pd);
		if(options.getBedfile() != null) {
			bedSingleRegionReport.getStatisticString(chrName, (int)winStart, winSize, pd.getNormalPosDepth(), "bed");
			genderSingleRegionReport.getStatisticString(chrName,(int) winStart, winSize, pd.getGenderPosDepth(), "gender");
		}
	}
	
	@Override
	public String toReducerString(String sample, String chrName, boolean unmappedRegion) {
		StringBuffer info = new StringBuffer();
		
		if(chrName == "-1") {
			info.append("sample:");
			info.append(sample);
			info.append("\n");
			info.append(basicReport.toReducerString());
			return info.toString();
		}
		info.append("sample:");
		info.append(sample);
		info.append("\n");
		info.append("chrName:");
		info.append(chrName);
		info.append("\n");
		info.append(basicReport.toReducerString());
		if(!unmappedRegion) {
			info.append(regionCoverReport.toReducerString());
			info.append(regionReport.toReducerString());
			if(options.isCnvDepth()) {
				//System.err.println("do cnv depth");
				info.append(cnvDepthReport.toReducerString());
			}
			info.append("insert size information:\n");
			insertSizeReportReducerString(info, insertSize);
			info.append("insert size information\n");
			
			info.append("insert size without dup information:\n");
			insertSizeReportReducerString(info, insertSizeWithoutDup);
			info.append("insert size without dup information\n");
			
			info.append("unmapped site information:\n");
			unmappedReport.toReducerString();
			info.append("unmapped site information\n");
			if(cnvSingleRegionReport != null)
				info.append(cnvSingleRegionReport.toReducerString());
			if(bedSingleRegionReport != null) 
				info.append(bedSingleRegionReport.toReducerString());
			if(genderSingleRegionReport != null) 
				info.append(genderSingleRegionReport.toReducerString());
		}
		return info.toString();
	}
	
	public int getSampleLaneSize(String sample) {
		return sampleLaneSize.get(sample);
	}
}
