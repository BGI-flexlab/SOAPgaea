package org.bgi.flexlab.gaea.tools.bamqualtiycontrol.report;

import java.io.IOException;
import java.util.ArrayList;

import org.bgi.flexlab.gaea.data.structure.positioninformation.depth.PositionDepth;
import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformationShare;
import org.bgi.flexlab.gaea.tools.mapreduce.bamqualitycontrol.BamQualityControlOptions;

public class WholeGenome extends OutputType{
	
	private CoverReport coverReport;
	
	public WholeGenome(BamQualityControlOptions options) throws IOException {
		super(options);
	}
	
	public boolean initCoverReport(ChromosomeInformationShare chrInfo) {
		if(null == chrInfo)
			return false;
		coverReport = CoverReport.getCoverReport(chrInfo);
		return true;
	}
	
	@Override
	public void depthReport(PositionDepth pd, int i, String chrName, long pos) {
		int depth = pd.getPosDepth(i);
		int noPCRdepth = pd.getRMDupPosDepth(i);
		super.regionCoverReport(depth, noPCRdepth);
		if(options.isOutputUnmapped() && depth != 0 )
			unmappedReport.updateUnmappedSites(pos, unmappedReport.getUnmappedSites(chrName));
		coverReport.constructDepthReport(pd, i);
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
			for(String key : CoverReport.getCoverReports().keySet()) {
				CoverReport cover = CoverReport.getCoverReport(key);
				info.append(cover.toReducerString());
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
		}
		return info.toString();
	}
	
}
