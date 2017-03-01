package org.bgi.flexlab.gaea.tools.bamqualtiycontrol.report;

import java.io.IOException;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

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
			
	private Map<String, WholeGenomeCoverReport> coverReports;
		
	public WholeGenomeResultReport(BamQualityControlOptions options) throws IOException {
		super(options);
	}
	
	public void initReports() throws IOException {
		super.initReports();
		coverReports = new ConcurrentHashMap<String, WholeGenomeCoverReport>();
	}
	
	public void initReports(String chrName) throws IOException {
		super.initReports();
		if(chrName.equals("-1"))
			return;
		else {
			coverReports = new ConcurrentHashMap<>();
			ChromosomeInformationShare chrInfo = genome.getChromosomeInfo(chrName);
			coverReports.put(chrName, new WholeGenomeCoverReport(chrInfo));
		}
	}
	
	@Override
	public void constructDepthReport(PositionDepth pd, int i, String chrName, long pos) {
		int depth = pd.getPosDepth(i);
		int noPCRdepth = pd.getRMDupPosDepth(i);
		super.regionCoverReport(depth, noPCRdepth);
		if(options.isOutputUnmapped() && depth != 0 )
			unmappedReport.updateUnmappedSites(pos, unmappedReport.getUnmappedSites(chrName));
		coverReports.get(chrName).constructDepthReport(pd, i);
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
			info.append(rmdupRegionCoverReport.toReducerString("RMDUP Region Depth"));
			for(String key : coverReports.keySet()) {
				WholeGenomeCoverReport cover = coverReports.get(key);
				info.append(cover.toReducerString(key));
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
	public void parseReport(LineReader lineReader, Text line, ReferenceShare genome) throws IOException {
		super.parseReport(lineReader, line, genome);
		String lineString = line.toString();
		if(lineString.contains("Cover Information")) {
			if(lineReader.readLine(line) > 0 && line.getLength() != 0) {
				String[] splitArray = line.toString().split("\t");
				WholeGenomeCoverReport coverReport = null;
				for(String keyValue : splitArray) {
					if(keyValue.split(" ").length == 1) {
						String chrName = keyValue;
						if(!coverReports.containsKey(chrName)) {
							ChromosomeInformationShare chrInfo = genome.getChromosomeInfo(chrName);
							coverReport = new WholeGenomeCoverReport(chrInfo);
							coverReports.put(chrName, coverReport);
						} else {
							coverReport = coverReports.get(chrName);
						}
					} else {
						coverReport.parse(keyValue, genome);
					}
				}
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
		TreeSet<String> keys = new TreeSet<String>(coverReports.keySet());
		for(String key : keys) {
			WholeGenomeCoverReport cover = coverReports.get(key);
			info.append(cover.toString(key));
		}
		reportwriter.write(info.toString().getBytes());
		reportwriter.close();
	}
}
