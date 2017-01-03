package org.bgi.flexlab.gaea.tools.mapreduce.realigner;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.bgi.flexlab.gaea.data.mapreduce.output.bam.GaeaBamOutputFormat;
import org.bgi.flexlab.gaea.data.mapreduce.writable.SamRecordWritable;
import org.bgi.flexlab.gaea.data.mapreduce.writable.WindowsBasedWritable;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.BioJob;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.ToolsRunner;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.WindowsBasedMapper;
import org.bgi.flexlab.gaea.tools.recalibrator.report.RecalibratorReportTableEngine;
import org.seqdoop.hadoop_bam.SAMFormat;

import htsjdk.samtools.SAMFileHeader;

public class Realigner extends ToolsRunner {

	public Realigner() {
		this.toolsDescription = "Gaea realigner\n";
	}

	private SAMFormat format = SAMFormat.BAM;
	
	private RealignerExtendOptions options = null;
	private RealignerOptions option = null;

	private int runRealigner(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		BioJob job = BioJob.getInstance();
		Configuration conf = job.getConfiguration();
		String[] remainArgs = remainArgs(args, conf);

		options = new RealignerExtendOptions();
		options.parse(remainArgs);

		option = options.getRealignerOptions();

		job.setJobName("GaeaRealigner");

		option.setHadoopConf(remainArgs, conf);

		// merge header and set to configuration
		SAMFileHeader header = job.setHeader(new Path(option.getRealignerInput()),
				new Path(option.getRealignerHeaderOutput()));

		job.setAnySamInputFormat(option.getInputFormat());
		job.setOutputFormatClass(GaeaBamOutputFormat.class);
		job.setOutputKeyValue(WindowsBasedWritable.class, SamRecordWritable.class, NullWritable.class,
				SamRecordWritable.class);

		job.setJarByClass(Realigner.class);
		job.setWindowsBasicMapperClass(WindowsBasedMapper.class, option.getWindowsSize());
		job.setReducerClass(RealignerReducer.class);
		job.setNumReduceTasks(option.getReducerNumber());

		FileInputFormat.setInputPaths(job, new Path(option.getRealignerInput()));
		FileOutputFormat.setOutputPath(job, new Path(option.getRealignerOutput()));

		if (job.waitForCompletion(true)) {
			if(options.isRecalibration())
				return mergeReportTable(options.getBqsrOptions(),header,null);
			return 0;
		}
		return 1;
	}

	private int runFixMate(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		BioJob job = BioJob.getInstance();
		job.setJobName("GaeaFixMate");

		Configuration conf = job.getConfiguration();
		String[] remainArgs = remainArgs(args, conf);
		option.setHadoopConf(remainArgs, conf);

		// merge header and set to configuration
		job.setHeader(new Path(option.getFixmateInput()), new Path(option.getFixmateHeaderOutput()));

		job.setAnySamInputFormat(format);
		job.setOutputFormatClass(GaeaBamOutputFormat.class);
		job.setOutputKeyValue(Text.class, SamRecordWritable.class, NullWritable.class, SamRecordWritable.class);

		job.setJarByClass(Realigner.class);
		job.setMapperClass(FixmateMapper.class);
		job.setReducerClass(FixmateReducer.class);
		job.setNumReduceTasks(option.getReducerNumber());

		FileInputFormat.setInputPaths(job, new Path(option.getFixmateInput()));
		FileOutputFormat.setOutputPath(job, new Path(option.getFixmateOutput()));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	private int mergeReportTable(RecalibratorOptions option, SAMFileHeader header, String out) {
		RecalibratorHdfsReportWriter writer = new RecalibratorHdfsReportWriter(out);
		RecalibratorReportTableEngine engine = new RecalibratorReportTableEngine(option, header, writer);
		engine.writeReportTable();
		return 0;
	}

	@Override
	public int run(String[] args) throws Exception {

		int res = runRealigner(args);

		if (res != 0) {
			throw new RuntimeException("Realigner is failed!");
		}

		res = runFixMate(args);

		return res;
	}
}
