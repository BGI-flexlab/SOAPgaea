package org.bgi.flexlab.gaea.tools.bamqualtiycontrol.report;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.bgi.flexlab.gaea.data.structure.reference.ReferenceShare;
import org.bgi.flexlab.gaea.tools.mapreduce.bamqualitycontrol.BamQualityControlOptions;

public class BamReport {
	
	private ReportBuilder reportBuilder;
	private ResultReport reportType;

	public void getOutput(BamQualityControlOptions options, Configuration conf, Path oPath) throws IOException {
		ReferenceShare genome = new ReferenceShare();
		genome.loadChromosomeList(options.getReferenceSequencePath());
		
		reportBuilder = new ReportBuilder();
		if ((options.getRegion() != null) || (options.getBedfile() != null))
			reportType = new RegionResultReport(options, conf);
		else
			reportType = new WholeGenomeResultReport(options);
		
		Map <String, ResultReport> reports = new ConcurrentHashMap<String, ResultReport>(); 
		FileSystem fs = oPath.getFileSystem(conf);
		FileStatus filelist[] = fs.listStatus(oPath);
		for(int i = 0; i < filelist.length; i++) {
			System.err.println(filelist[i].getPath());
			if(!filelist[i].isDir() && !filelist[i].getPath().toString().startsWith("_")) {
				FSDataInputStream reader = fs.open(filelist[i].getPath());
				LineReader lineReader = new LineReader(reader, conf);
				Text line = new Text();
				while(lineReader.readLine(line) > 0) {
					String lineString = line.toString();
					if(line.getLength() == 0) {
						continue;
					}
					
					if(lineString.contains("sample:")) {
						String[] sampleSplit = line.toString().split(":");
						if(!reports.containsKey(sampleSplit[1])) {
							reports.put(sampleSplit[1], reportType);
						} else {
							reportType = reports.get(sampleSplit[1]);
						}
					}
					
					reportBuilder.setReportChoice(reportType);
					
					reportBuilder.parseReport(lineReader, line, genome);
						
					lineReader.close();
					reader.close();
				}
			}	
		}
		
		for(String sampleName:reports.keySet()) {
			System.err.println("sample:" + sampleName);
			ResultReport report = reports.get(sampleName);
			report.write(fs, sampleName);
		}	
		
		fs.close();
	}
}
