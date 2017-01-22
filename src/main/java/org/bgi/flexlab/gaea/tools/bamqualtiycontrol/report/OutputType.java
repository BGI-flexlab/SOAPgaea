package org.bgi.flexlab.gaea.tools.bamqualtiycontrol.report;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.bgi.flexlab.gaea.data.structure.positioninformation.depth.PositionDepth;
import org.bgi.flexlab.gaea.data.structure.reference.ReferenceShare;
import org.bgi.flexlab.gaea.data.structure.region.SingleRegion;
import org.bgi.flexlab.gaea.data.structure.region.report.CNVSingleRegionReport;
import org.bgi.flexlab.gaea.data.structure.region.report.RegionCoverReport;
import org.bgi.flexlab.gaea.tools.bamqualtiycontrol.SamRecordDatum;
import org.bgi.flexlab.gaea.tools.mapreduce.bamqualitycontrol.BamQualityControlOptions;

public abstract class OutputType {
	
	protected BasicReport basicReport;
	
	protected BamQualityControlOptions options;
	
	protected RegionCoverReport regionCoverReport;
	
	protected RegionCoverReport rmdupRegionCoverReport;
	
	protected CNVSingleRegionReport cnvSingleRegionReport;
	
	protected UnmappedReport unmappedReport;
	
	protected int[] insertSize = new int[2000];
	
	protected int[] insertSizeWithoutDup = new int[2000];
	
	public OutputType(BamQualityControlOptions options) throws IOException {
		this.options = options;
		basicReport = new BasicReport();
		unmappedReport = new UnmappedReport();
		regionCoverReport = new RegionCoverReport(1000);
		rmdupRegionCoverReport = new RegionCoverReport(1000);
		
		if(options.getSingleRegion() != null) {
			SingleRegion singleRegion = new SingleRegion();
			singleRegion.parseRegionsFileFromHDFS(options.getCnvRegion(), false, 0);
			cnvSingleRegionReport = new CNVSingleRegionReport(singleRegion);
		}
		Arrays.fill(insertSize, 0);
		Arrays.fill(insertSizeWithoutDup, 0);
	}
	
	public boolean unmappedReport(long winNum, String chrName, Iterable<Text> values) {
		return unmappedReport.constructMapReport(winNum, chrName, values, basicReport);
	}
	
	public boolean mappedReport(SamRecordDatum datum, ReferenceShare genome, String chrName, Context context) {
		return basicReport.constructMapReport(datum, genome, chrName, context);
	}
	
	public void regionCoverReport(int depth, int noPCRdepth) {
		regionCoverReport.add(depth);
		rmdupRegionCoverReport.add(noPCRdepth);
	}
	
	public void finalizeUnmappedReport(String chrName) {
		if(options.isOutputUnmapped()) {
			unmappedReport.finalize(unmappedReport.getUnmappedSites(chrName));
		}
	}
	
	public void singleRegionReports(String chrName, long winStart, int winSize , PositionDepth pd) {
		if(options.getSingleRegion() != null)
			cnvSingleRegionReport.getStatisticString(chrName, (int) winStart, winSize, pd.getRMDupPosDepth(), "cnv");
	}
	
	public void insertReport(SamRecordDatum datum) {
		if((datum.getFlag() & 0x40) != 0) {
			int insert = datum.getInsertSize();
			if(Math.abs(insert) < 2000) {
				insertSize[Math.abs(insert)]++;
				if(!datum.isDup()) {
					insertSizeWithoutDup[Math.abs(insert)]++;
				}
			}
		}
	}
	
	protected void insertSizeReportReducerString(StringBuffer info, int[] insertSize) {
		for(int i = 0; i < insertSize.length; i++) {
			if(insertSize[i] != 0) {
				info.append(i);
				info.append("\t");
				info.append(insertSize[i]);
				info.append("\n");
			}
		}
	}
	
	protected abstract void depthReport(PositionDepth pd, int i, String chrName, long pos);

	public abstract String toReducerString(String sample, String chrName, boolean unmappedRegion);
}
