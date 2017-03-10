package org.bgi.flexlab.gaea.tools.mapreduce.realigner;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.bgi.flexlab.gaea.data.mapreduce.output.bam.GaeaBamOutputFormat;
import org.bgi.flexlab.gaea.data.mapreduce.writable.SamRecordWritable;
import org.bgi.flexlab.gaea.data.mapreduce.writable.WindowsBasedWritable;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.BioJob;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.ToolsRunner;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.WindowsBasedSamRecordMapper;
import org.bgi.flexlab.gaea.tools.recalibrator.report.RecalibratorReportTableEngine;
import org.seqdoop.hadoop_bam.SAMFormat;

import htsjdk.samtools.SAMFileHeader;

public class Realigner extends ToolsRunner {

	public Realigner() {
		this.toolsDescription = "Gaea realigner\n";
	}

	public final static String RECALIBRATOR_REPORT_TABLE_NAME = "bqsr.report.table";

	private final static SAMFormat format = SAMFormat.BAM;

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
		job.setWindowsBasicMapperClass(WindowsBasedSamRecordMapper.class, option.getWindowsSize());
		job.setReducerClass(RealignerReducer.class);
		job.setNumReduceTasks(option.getReducerNumber());

		FileInputFormat.setInputPaths(job, new Path(option.getRealignerInput()));
		FileOutputFormat.setOutputPath(job, new Path(option.getRealignerOutput()));

		if(options.isRecalibration())
			MultipleOutputs.addNamedOutput(job, RecalibratorContextWriter.RECALIBRATOR_TABLE_TAG, TextOutputFormat.class,
				NullWritable.class, Text.class);

		if (job.waitForCompletion(true)) {
			if (options.isRecalibration())
				return mergeReportTable(options.getBqsrOptions(), header,
						options.getReportOutput() + RECALIBRATOR_REPORT_TABLE_NAME);
			return 0;
		}
		
		return 1;
	}

	private int runFixMate(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		BioJob job = BioJob.getInstance();
		job.setJobName("GaeaFixMate");

		Configuration conf = job.getConfiguration();

		// set bqsr table path
		if (options.isRecalibration())
			conf.set(RECALIBRATOR_REPORT_TABLE_NAME, options.getReportOutput() + RECALIBRATOR_REPORT_TABLE_NAME);

		String[] remainArgs = remainArgs(args, conf);
		option.setHadoopConf(remainArgs, conf);

		// merge header and set to configuration
		job.setHeader(new Path(option.getFixmateInput()), new Path(option.getFixmateHeaderOutput()));

		job.setAnySamInputFormat(format);
		job.setOutputFormatClass(GaeaBamOutputFormat.class);

		job.setJarByClass(Realigner.class);
		job.setMapperClass(FixmateMapper.class);
		job.setReducerClass(FixmateReducer.class);

		if (!options.isRealignment()) {
			job.setNumReduceTasks(0);
			job.setOutputKeyValue(NullWritable.class, SamRecordWritable.class, NullWritable.class,
					SamRecordWritable.class);
		} else {
			job.setOutputKeyValue(Text.class, SamRecordWritable.class, NullWritable.class, SamRecordWritable.class);
			job.setNumReduceTasks(option.getReducerNumber());
		}

		FileInputFormat.setInputPaths(job, new Path(option.getFixmateInput()));
		FileOutputFormat.setOutputPath(job, new Path(option.getFixmateOutput()));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	private int mergeReportTable(RecalibratorOptions bqsrOption, SAMFileHeader header, String output) {
		RecalibratorHdfsReportWriter writer = new RecalibratorHdfsReportWriter(output);
		RecalibratorReportTableEngine engine = new RecalibratorReportTableEngine(bqsrOption, header, writer);
		engine.writeReportTable(option.getRealignerOutput());
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
