package org.bgi.flexlab.gaea.tools.bamqualtiycontrol.report;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.util.LineReader;
import org.bgi.flexlab.gaea.data.structure.positioninformation.depth.PositionDepth;
import org.bgi.flexlab.gaea.data.structure.reference.ReferenceShare;
import org.bgi.flexlab.gaea.data.structure.region.SingleRegion;
import org.bgi.flexlab.gaea.data.structure.region.SingleRegion.Regiondata;
import org.bgi.flexlab.gaea.data.structure.region.statistic.CNVSingleRegionStatistic;
import org.bgi.flexlab.gaea.tools.bamqualtiycontrol.counter.BaseCounter;
import org.bgi.flexlab.gaea.tools.bamqualtiycontrol.counter.ReadsCounter;
import org.bgi.flexlab.gaea.tools.bamqualtiycontrol.counter.Tracker.BaseTracker;
import org.bgi.flexlab.gaea.tools.bamqualtiycontrol.counter.Tracker.ReadsTracker;
import org.bgi.flexlab.gaea.tools.mapreduce.bamqualitycontrol.BamQualityControlOptions;
import org.bgi.flexlab.gaea.util.SamRecordDatum;

public abstract class ResultReport {
	
	protected BasicReport basicReport;
	
	protected BamQualityControlOptions options;
	
	protected RegionCoverReport regionCoverReport;
	
	protected RegionCoverReport rmdupRegionCoverReport;
	
	protected CNVSingleRegionReport cnvSingleRegionReport;
	
	protected UnmappedReport unmappedReport;
	
	protected int[] insertSize;
	
	protected int[] insertSizeWithoutDup;
	
	protected ReferenceShare genome;
	
	public ResultReport(BamQualityControlOptions options) throws IOException {
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
		insertSize = new int[options.getInsertSzie()];
		insertSizeWithoutDup = new int[options.getInsertSzieWithoutDup()];
		genome = new ReferenceShare();
		loadReference();
		Arrays.fill(insertSize, 0);
		Arrays.fill(insertSizeWithoutDup, 0);
	}
	
	private void loadReference() {
		genome = new ReferenceShare();
		if(options.isDistributeCache()) {
			genome.loadChromosomeList();
		} else {
			genome.loadChromosomeList(options.getReferenceSequencePath());
		}
	}
	public boolean unmappedReport(long winNum, String chrName, Iterable<Text> values) {
		return unmappedReport.constructMapReport(winNum, chrName, values, basicReport);
	}
	
	public boolean mappedReport(SamRecordDatum datum, String chrName, Context context) {
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
	
	public void parseLine(String line, ReadsTracker rTracker, BaseTracker bTracker) {
		String[] splitArray = line.split("\t");
		for(String keyValue : splitArray)
			parseKeyValue(keyValue, rTracker, bTracker);
	}
	
	private void parseKeyValue(String keyValue, ReadsTracker rTracker, BaseTracker bTracker) {
		String key = keyValue.split("\t")[0];
		String value = keyValue.split("\t")[1];
		ReadsCounter rCounter = null;
		BaseCounter bCounter = null;
		if((rCounter = rTracker.getCounterMap().get(key)) != null)
			rCounter.setReadsCount(Long.parseLong(value));
		else if((bCounter = bTracker.getCounterMap().get(key)) != null)
			bCounter.setBaseCount(Long.parseLong(value));
		else {
			throw new RuntimeException("Can not idenity counter with name " + key);
		}
			
	}
	
	public abstract void depthReport(PositionDepth pd, int i, String chrName, long pos);

	public abstract String toReducerString(String sample, String chrName, boolean unmappedRegion);

	public void parseReport(LineReader lineReader, Text line, ReferenceShare genome) throws IOException {
		String lineString = line.toString();
		String chrName = "";
		if(lineString.contains("chrName:")) {
			String[] sampleSplit = line.toString().split(":");
			chrName = sampleSplit[1];
		}
		
		if(lineString.contains("Basic Information")) {
			if(lineReader.readLine(line) > 0 && line.getLength() != 0) {
				basicReport.parse(line.toString());
			}
		}
		
		if(lineString.startsWith("cnv part single Region Statistic")) {
			if(lineReader.readLine(line) > 0 && line.getLength() != 0) {
				cnvSingleRegionReport.parseReducerOutput(line.toString(), true);
			}
		}
		if(lineString.startsWith("cnv single Region Statistic")) {
			if(lineReader.readLine(line) > 0 && line.getLength() != 0) {
				cnvSingleRegionReport.parseReducerOutput(line.toString(), false);
			}
		}
		if(lineString.startsWith("Rgion Depth")) {
			if(lineReader.readLine(line) > 0 && line.getLength() != 0) {
				regionCoverReport.parseReducerOutput(line.toString());
			}
		}
		if(lineString.startsWith("RMDUP Rgion Depth")) {
			if(lineReader.readLine(line) > 0 && line.getLength() != 0) {
				rmdupRegionCoverReport.parseReducerOutput(line.toString());
			}
		}
		
		if(lineString.contains("insert size information")) {
			fillInsertSize(lineReader, line, insertSize);
		}
		
		if(lineString.contains("insert size without dup information")) {
			fillInsertSize(lineReader, line, insertSizeWithoutDup);
		}
		
		if(lineString.contains("unmapped site information") && options.isOutputUnmapped()) {
			String[] splitArray = null;
			ArrayList<Long> unmappedSites =  unmappedReport.getUnmappedSites(chrName);
			while(lineReader.readLine(line) > 0 && line.getLength() != 0) {
				if(line.toString().contains("unmapped site information")) {
					break;
				}
				splitArray = line.toString().split("\t");
				
				unmappedSites.add(Long.parseLong(splitArray[0]));
				unmappedSites.add(Long.parseLong(splitArray[1]));
			}
		}
	}
	
	public void write(FileSystem fs, String sampleName) throws IOException {
		if(cnvSingleRegionReport != null) {
			StringBuffer singleRegionFilePath = new StringBuffer();
			singleRegionFilePath.append(options.getOutputPath());
			singleRegionFilePath.append("/");
			singleRegionFilePath.append(sampleName);
			singleRegionFilePath.append(".anno_region.txt");
			Path singleRegionPath = new Path(singleRegionFilePath.toString());
			FSDataOutputStream singleRegionwriter = fs.create(singleRegionPath);
			
			StringBuffer unsingleRegionFilePath = new StringBuffer();
			unsingleRegionFilePath.append(options.getOutputPath());
			unsingleRegionFilePath.append("/");
			unsingleRegionFilePath.append(sampleName);
			unsingleRegionFilePath.append(".anno_region_low_depth.txt");
			Path unsingleRegionPath = new Path(unsingleRegionFilePath.toString());
			FSDataOutputStream unsingleRegionwriter = fs.create(unsingleRegionPath);
			
			Map<Regiondata, CNVSingleRegionStatistic> result = cnvSingleRegionReport.getResult();
			cnvSingleRegionReport.updateAllRegionAverageDeepth();
			for(Regiondata regionData : result.keySet()) {
				if(result.get(regionData).getDepth(regionData) > options.getMinSingleRegionDepth())
					singleRegionwriter.write(result.get(regionData).toString(regionData, false, cnvSingleRegionReport.getAllRegionAverageDeepth()).getBytes());
				else {
					singleRegionwriter.write(result.get(regionData).toString(regionData, false, cnvSingleRegionReport.getAllRegionAverageDeepth()).getBytes());
					unsingleRegionwriter.write(result.get(regionData).toString(regionData, false, cnvSingleRegionReport.getAllRegionAverageDeepth()).getBytes());
				}
			}
			singleRegionwriter.close();
			unsingleRegionwriter.close();
		}
		
		if(regionCoverReport != null) {
			StringBuffer RegionDepthFilePath = new StringBuffer();
			RegionDepthFilePath.append(options.getOutputPath());
			RegionDepthFilePath.append("/");
			RegionDepthFilePath.append(sampleName);
			RegionDepthFilePath.append(".depth.txt");
			Path regionDepthPath = new Path(RegionDepthFilePath.toString());
			FSDataOutputStream regionDepthWriter = fs.create(regionDepthPath);
			int[] depth = regionCoverReport.getDepthArray();
			
			if(rmdupRegionCoverReport != null)
			{
				int[] rmdepth = rmdupRegionCoverReport.getDepthArray();

				StringBuilder sb = new StringBuilder();
				for(int i = 0; i < depth.length; i++) {
			             sb.append(i);
			             sb.append("\t");
				     sb.append(depth[i]);
			             sb.append("\t");
				     sb.append(rmdepth[i]);
			             sb.append("\n");
				}
				regionDepthWriter.write(sb.toString().getBytes());
			}else{
				regionDepthWriter.write(regionCoverReport.toString().getBytes());
			}
			regionDepthWriter.close();
		}
		
		StringBuffer reportFilePath = new StringBuffer();
		reportFilePath.setLength(0);
		reportFilePath.append(options.getOutputPath());
		reportFilePath.append("/");
		reportFilePath.append(sampleName);
		reportFilePath.append(".unmapped.bed");
		Path bedPath = new Path(reportFilePath.toString());
		FSDataOutputStream bedwriter = fs.create(bedPath);
		
		StringBuilder bedString = new StringBuilder();
		for(String chrName:unmappedReport.getUnmappedSites().keySet()) {
			ArrayList<Long> sites = unmappedReport.getUnmappedSites(chrName);
			for(int i = 0; i < sites.size(); i += 2) {
				bedString.append(chrName);
				bedString.append("\t");
				bedString.append(sites.get(i));
				bedString.append("\t");
				bedString.append(sites.get(i+1));
				bedString.append("\n");
			}
		}
		bedwriter.write(bedString.toString().getBytes());
		bedwriter.close();
		
		reportFilePath.setLength(0);
		reportFilePath.append(options.getOutputPath());
		reportFilePath.append("/");
		reportFilePath.append(sampleName);
		reportFilePath.append(".insert.xls");
		Path insertPath = new Path(reportFilePath.toString());
		FSDataOutputStream insertwriter = fs.create(insertPath);
		
		StringBuilder insertString = new StringBuilder();
		for(int i = 0; i < insertSize.length; i++) {
			insertString.append(i);
			insertString.append("\t");
			insertString.append(insertSize[i]);
			insertString.append("\n");
		}
		insertwriter.write(insertString.toString().getBytes());
		insertwriter.close();
		
		reportFilePath.setLength(0);
		reportFilePath.append(options.getOutputPath());
		reportFilePath.append("/");
		reportFilePath.append(sampleName);
		reportFilePath.append(".insert_without_dup.xls");
		Path insertWithoutDupPath = new Path(reportFilePath.toString());
		FSDataOutputStream insertWithoutDupwriter = fs.create(insertWithoutDupPath);
		
		StringBuilder insertWithoutDupString = new StringBuilder();
		for(int i = 0; i < insertSizeWithoutDup.length; i++) {
			insertWithoutDupString.append(i);
			insertWithoutDupString.append("\t");
			insertWithoutDupString.append(insertSizeWithoutDup[i]);
			insertWithoutDupString.append("\n");
		}
		insertWithoutDupwriter.write(insertWithoutDupString.toString().getBytes());
		insertWithoutDupwriter.close();
	}
	
	private void fillInsertSize(LineReader lineReader, Text line, int[] insertSize) throws RuntimeException, IOException {
		String[] splitArray = null;
		while(lineReader.readLine(line) > 0 && line.getLength() != 0) {
			if(line.toString().contains("insert size ")) {
				break;
			}
			splitArray = line.toString().split("\t");
			int index = Integer.parseInt(splitArray[0]);
			insertSize[index] += Integer.parseInt(splitArray[1]);
		}
	}
	
	public CNVSingleRegionReport getCNVSingleRegionReport() {
		return cnvSingleRegionReport;
	}
	
	public ReferenceShare getReference() {
		return genome;
	}
}
