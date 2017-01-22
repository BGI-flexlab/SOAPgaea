package org.bgi.flexlab.gaea.tools.bamqualtiycontrol.report;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.bgi.flexlab.gaea.data.structure.positioninformation.depth.PositionDepth;
import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformationShare;
import org.bgi.flexlab.gaea.data.structure.reference.ReferenceShare;
import org.bgi.flexlab.gaea.tools.bamqualtiycontrol.SamRecordDatum;

public class ReportBuilder {
	
	OutputType report;
	
	public void setReportChoice(OutputType report) {
		this.report = report;
	}
	
	public void initCNVDepthReport(String sampleName) throws IOException {
		((Region) report).initCNVDepthReport(sampleName);
	}
	
	public boolean initCoverReport(ChromosomeInformationShare chrInfo) {
		return ((WholeGenome) report).initCoverReport(chrInfo);
	}
	
	public boolean unmappedReport(long winNum, String chrName, Iterable<Text> values) {
		return report.unmappedReport(winNum, chrName, values);
	}
	
	public void finalizeUnmappedReport(String chrName) {
		report.finalizeUnmappedReport(chrName);
	}
	
	public boolean mappedReport(SamRecordDatum datum, ReferenceShare genome, String chrName, Context context) {
		return report.mappedReport(datum, genome, chrName, context);
	}
	
	public void depthReport(PositionDepth pd, int i, String chrName, long pos) {
		report.depthReport(pd, i, chrName, pos);
	}
	
	public void singleRegionReports(String chrName, long winStart, int winSize , PositionDepth pd) {
		report.singleRegionReports(chrName, winStart, winSize, pd);
	}
	
	public void insertReport(SamRecordDatum datum) {
		report.insertReport(datum);
	}
	
	public int getSampleLaneSzie(String sample) {
		return report instanceof Region ? ((Region) report).getSampleLaneSize(sample) : 0;
	}
	
	public OutputType build() {
		return report;
	}
}
