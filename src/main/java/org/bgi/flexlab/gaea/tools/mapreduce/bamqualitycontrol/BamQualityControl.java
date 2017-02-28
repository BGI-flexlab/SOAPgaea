package org.bgi.flexlab.gaea.tools.mapreduce.bamqualitycontrol;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.bgi.flexlab.gaea.data.mapreduce.input.bam.GaeaAnySAMInputFormat;
import org.bgi.flexlab.gaea.data.mapreduce.input.header.SamHdfsFileHeader;
import org.bgi.flexlab.gaea.data.structure.reference.ReferenceShare;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.BioJob;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.ToolsRunner;
import org.bgi.flexlab.gaea.tools.bamqualtiycontrol.report.BamReport;

public class BamQualityControl extends ToolsRunner{
	
	public final static int WINDOW_SIZE = 1000000; 
	
	public BamQualityControl() {
			this.toolsDescription = "Gaea bam quality control\n"
					+ "The purpose of bam quality control is to attain statistics information"
					+ "of the bam file";
	}
	 
	private BamQualityControlOptions options;
	
	@Override
	public int run(String[] args) throws Exception {
		BioJob job = BioJob.getInstance();
		Configuration conf = job.getConfiguration();
		
		options = new BamQualityControlOptions();
		options.parse(args);
		options.setHadoopConf(args, conf);
		
		if(options.isDistributeCache()) {
			ReferenceShare.distributeCache(options.getReferenceSequencePath(), job);
		}
		SamHdfsFileHeader.loadHeader(new Path(options.getAlignmentFilePath()), conf, new Path(options.getOutputPath()));
		
		job.setJarByClass(BamQualityControl.class);
		job.setMapperClass(BamQualityControlMapper.class);
		job.setReducerClass(BamQualityControlReducer.class);
		job.setOutputKeyValue(Text.class, Text.class, 
				NullWritable.class, Text.class);
		job.setNumReduceTasks(options.getReducerNum());
		
		FileInputFormat.addInputPaths(job, options.getAlignmentFilePath());
		job.setInputFormatClass(GaeaAnySAMInputFormat.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileOutputFormat.setOutputPath(job, new Path(options.getTempPath()));
		if(job.waitForCompletion(true)) {
			BamReport.getOutput(options, conf, new Path(options.getTempPath()));
			return 0;
		} else {
			return 1;
		}
	}
	
	public static void main(String[] args) throws Exception {
		BamQualityControl bamqc = new BamQualityControl();
		bamqc.run(args);
	}
}
