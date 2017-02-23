package org.bgi.flexlab.gaea.tools.bamqualtiycontrol.report;

import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.bgi.flexlab.gaea.data.structure.positioninformation.depth.PositionDepth;
import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformationShare;
import org.bgi.flexlab.gaea.data.structure.reference.ReferenceShare;
import org.bgi.flexlab.gaea.tools.mapreduce.bamqualitycontrol.BamQualityControlOptions;

public class WholeGenomeResultReport extends ResultReport{
	
	private WholeGenomeCoverReport coverReport;
	
	public WholeGenomeResultReport(BamQualityControlOptions options) throws IOException {
		super(options);
	}
	
	public void initReports(String chrName) throws IOException {
		super.initReports();
		if(chrName.equals("-1"))
			return;
		else {
			ChromosomeInformationShare chrInfo = genome.getChromosomeInfo(chrName);
			coverReport = WholeGenomeCoverReport.getCoverReport(chrInfo);
		}
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
			for(String key : WholeGenomeCoverReport.getCoverReports().keySet()) {
				WholeGenomeCoverReport cover = WholeGenomeCoverReport.getCoverReport(key);
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
	
	@Override
	public void parseReport(String sample, LineReader lineReader, Text line, ReferenceShare genome) throws IOException {
		super.initReports();
		super.parseReport(sample, lineReader, line, genome);
		String lineString = line.toString();
		if(lineString.contains("Cover Information")) {
			if(lineReader.readLine(line) > 0 && line.getLength() != 0) {
				coverReport.parse(line.toString(), genome);
			}
		}
		
	}
	
	@Override
	public void write(FileSystem fs, String sampleName) throws IOException {
		super.write(fs, sampleName);
		StringBuffer reportFilePath = new StringBuffer();
		reportFilePath.append(options.getOutputPath());
		reportFilePath.append("/");
		reportFilePath.append(sampleName);
		reportFilePath.append(".bam.report.txt");
		Path reportPath = new Path(reportFilePath.toString());
		FSDataOutputStream reportwriter = fs.create(reportPath);
		StringBuffer info = new StringBuffer();
		info.append(basicReport.toString());
		info.append("coverage information:\n");
		TreeSet<String> keys = new TreeSet<String>(WholeGenomeCoverReport.getCoverReports().keySet());
		for(String key : keys) {
			WholeGenomeCoverReport cover = WholeGenomeCoverReport.getCoverReports().get(key);
			info.append(cover.toString());
		}
		reportwriter.write(info.toString().getBytes());
		reportwriter.close();
	}
}
