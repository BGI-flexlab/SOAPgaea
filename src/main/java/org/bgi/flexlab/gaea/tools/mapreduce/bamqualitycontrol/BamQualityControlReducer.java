package org.bgi.flexlab.gaea.tools.mapreduce.bamqualitycontrol;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bgi.flexlab.gaea.data.structure.positioninformation.depth.PositionDepth;
import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformationShare;
import org.bgi.flexlab.gaea.tools.bamqualtiycontrol.report.ResultReport;
import org.bgi.flexlab.gaea.tools.bamqualtiycontrol.report.RegionResultReport;
import org.bgi.flexlab.gaea.tools.bamqualtiycontrol.report.ReportBuilder;
import org.bgi.flexlab.gaea.tools.bamqualtiycontrol.report.WholeGenomeResultReport;
import org.bgi.flexlab.gaea.util.SamRecordDatum;

public class BamQualityControlReducer extends Reducer<Text, Text, NullWritable, Text>{
	
	private BamQualityControlOptions options;
		
	private ResultReport reportType;
	
	private ReportBuilder reportBuilder;
		
	private PositionDepth deep;
				
	@Override
	protected void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		options.getOptionsFromHadoopConf(conf);
		
		reportBuilder = new ReportBuilder();
		if ((options.getRegion() != null) || (options.getBedfile() != null))
			reportType = new RegionResultReport(options, conf);
		else
			reportType = new WholeGenomeResultReport(options);
	
	}
	
	@Override
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		String[] keySplit = key.toString().split(":");	// 按“:”分割Key值
		String sampleName = keySplit[0];	//样品名
		String chrName = keySplit[1];					// 染色体名称
		long winNum = Long.parseLong(keySplit[2]);		// 窗口编号
		ChromosomeInformationShare chrInfo = null;
		try{
			chrInfo = reportType.getReference().getChromosomeInfo(chrName);
		} catch (Exception e) {
			if(chrName.equals("-1")) {
//				do nothing, it means unmapped area
			} else {
				throw new RuntimeException(e);
			}
		}
		reportBuilder.setReportChoice(reportType);
		reportBuilder.initReports(sampleName, chrName);
		
		// 按名称获取染色体信息
		if(reportBuilder.unmappedReport(winNum, chrName, values)) {
			ResultReport report = reportBuilder.build();
			context.write(NullWritable.get(), new Text(report.toReducerString(sampleName, chrName, true)));
			return;
		}
		
		long start = winNum * BamQualityControl.WINDOW_SIZE; // 窗口起始坐标

		// 如果窗口起始坐标大于染色体的长度值，则退出程序
		if(start > chrInfo.getLength()) {
			context.getCounter("Exception", "window start beyond chromosome").increment(1);
			return;
		}
		
		long winStart = start; // 窗口起始坐标
		long readPos;	// Read上第一个碱基在参考基因组上的坐标值
		long winEnd = start + BamQualityControl.WINDOW_SIZE - 1;
		
		int winSize = BamQualityControl.WINDOW_SIZE;
		if(winEnd > chrInfo.getLength()) {
			winSize = (int) (chrInfo.getLength() - start);
			winEnd = chrInfo.getLength() - 1;
			context.getCounter("Exception", "window end > chr length:" ).increment(1);
		}
		//position depth
		deep = new PositionDepth(winSize, options.isGenderDepth(), reportBuilder.getSampleLaneSzie(sampleName));
		
		for(Text value : values) {
			SamRecordDatum datum = new SamRecordDatum();

			if(!datum.parseBAMQC(value.toString())) {
				context.getCounter("Exception", "parse mapper output error").increment(1);
				continue;
			}
			readPos = datum.getPosition(); // Read上第一个碱基在参考基因组上的坐标值
			// 如果read起始坐标小于0，则不处理该read，进入下一次循环
			if (readPos < 0) {
				context.getCounter("Exception", "read start pos less than zero").increment(1);
				continue;
			}
			
			if(!deep.add((int)winStart, winSize, datum)) 
				context.getCounter("Exception", "null read info in depth class").increment(1);
			
			if(datum.isRepeat()) 
				continue;
			
			if (!reportBuilder.mappedReport(datum, chrName, context)) 
				continue;
				
			reportBuilder.insertReport(datum);
		}
				
		for(int i = 0; i < winSize; i++) {
			long pos = winStart + i;
			//深度和覆盖度统计
			int depth = deep.getPosDepth(i);
			if(depth < 0) {
				throw new RuntimeException("sample:" + sampleName + " chr:" + chrName + " windSize:" + winSize + " position:" + i + " winStart:" + winStart + " depth:" + depth + " < 0.");
			}
			reportBuilder.depthReport(deep, i, chrName, pos);
		}
		
		//last unmapped sites
		//BED format: 0,1 stand for 1 position
		if(options.isOutputUnmapped()) {
			reportBuilder.finalizeUnmappedReport(chrName);
		}
		//sum single region
		reportBuilder.singleRegionReports(chrName, winStart, winSize, deep);
		
		//write reducer
		ResultReport report = reportBuilder.build();
		context.write(NullWritable.get(), new Text(report.toReducerString(sampleName, chrName, false)));
	} 

}
