package org.bgi.flexlab.gaea.tools.mapreduce.realigner;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.bgi.flexlab.gaea.data.mapreduce.output.bam.GaeaBamOutputFormat;
import org.bgi.flexlab.gaea.data.mapreduce.writable.WindowsBasedWritable;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.BioJob;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.ToolsRunner;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.WindowsBasedMapper;
import org.seqdoop.hadoop_bam.SAMFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

public class Realigner extends ToolsRunner {

	public Realigner() {
		this.toolsDescription = "Gaea realigner\n";
	}

	private SAMFormat format = SAMFormat.BAM;
	private RealignerOptions option = null;

	public int runRealigner(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		BioJob job = BioJob.getInstance();
		Configuration conf = job.getConfiguration();
		String[] remainArgs = remainArgs(args,conf);
		
		option = new RealignerOptions();
		option.parse(remainArgs);

		job.setJobName("GaeaRealigner");
		
		option.setHadoopConf(args, conf);

		job.setAnySamInputFormat(option.getInputFormat());
		job.setOutputFormatClass(GaeaBamOutputFormat.class);
		job.setOutputKeyValue(WindowsBasedWritable.class,
				SAMRecordWritable.class, NullWritable.class,
				SAMRecordWritable.class);

		job.setJarByClass(Realigner.class);
		job.setMapperClass(WindowsBasedMapper.class);
		job.setReducerClass(RealignerReducer.class);
		job.setNumReduceTasks(option.getReducerNumber());

		FileInputFormat
				.setInputPaths(job, new Path(option.getRealignerInput()));
		FileOutputFormat.setOutputPath(job,
				new Path(option.getRealignerOutput()));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public int runFixMate(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {

		BioJob job = BioJob.getInstance();
		job.setJobName("GaeaFixMate");

		Configuration conf = job.getConfiguration();
		String[] remainArgs = remainArgs(args,conf);
		option.setHadoopConf(remainArgs, conf);

		job.setAnySamInputFormat(format);
		job.setOutputFormatClass(GaeaBamOutputFormat.class);
		job.setOutputKeyValue(Text.class, SAMRecordWritable.class,
				NullWritable.class, SAMRecordWritable.class);

		job.setJarByClass(Realigner.class);
		job.setMapperClass(FixmateMapper.class);
		job.setReducerClass(FixmateReducer.class);
		job.setNumReduceTasks(option.getReducerNumber());

		FileInputFormat.setInputPaths(job, new Path(option.getFixmateInput()));
		FileOutputFormat
				.setOutputPath(job, new Path(option.getFixmateOutput()));

		return job.waitForCompletion(true) ? 0 : 1;
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
