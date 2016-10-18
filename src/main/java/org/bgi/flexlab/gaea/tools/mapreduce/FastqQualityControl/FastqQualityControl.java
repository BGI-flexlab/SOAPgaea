package org.bgi.flexlab.gaea.tools.mapreduce.fastqqualitycontrol;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.bgi.flexlab.gaea.data.mapreduce.input.adaptor.AdaptorInputFormat;
import org.bgi.flexlab.gaea.data.mapreduce.input.fastq.FastqInputFormat;
import org.bgi.flexlab.gaea.data.mapreduce.input.fastq.FastqMultipleSample;
import org.bgi.flexlab.gaea.data.mapreduce.input.fastq.FastqRecordReader;
import org.bgi.flexlab.gaea.data.mapreduce.input.fastq.FastqSample;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.BioJob;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.PairEndAggregatorMapper;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.ToolsRunner;
import org.bgi.flexlab.gaea.tools.fastqqualitycontrol.report.FastqQualityControlReporterIO;

public class FastqQualityControl extends ToolsRunner {

	public FastqQualityControl() {
		this.toolsDescription = "Gaea fastq quality control\n";
	}

	@Override
	public int run(String[] args) throws Exception {
		FastqQualityControlOptions option = new FastqQualityControlOptions();
		option.parse(args);

		BioJob job = BioJob.getInstance();
		Configuration conf = job.getConfiguration();
		conf.setInt(FastqRecordReader.READ_NAME_TYPE, option.getReadType());
		option.setHadoopConf(args, conf);

		job.setJobName("GaeaFastqQC");
		job.setJarByClass(FastqQualityControl.class);
		job.setMapperClass(PairEndAggregatorMapper.class);
		job.setReducerClass(FastqQualityControlReducer.class);

		job.setInputFormatClass(FastqInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(option.getReducerNumber());
		job.setOutputKeyValue(Text.class, Text.class, NullWritable.class,
				Text.class);

		FastqMultipleSample sample = null;
		if (option.getMultiSampleList() != null
				&& option.getMultiSampleList() != "") {
			sample = new FastqMultipleSample(option.getMultiSampleList(), true);
			Map<String, FastqSample> sampleList = sample.getSampleList();

			for (FastqSample sl : sampleList.values()) {
				if (sl.getFastq1() != null) {
					MultipleInputs.addInputPath(job, new Path(sl.getFastq1()),
							FastqInputFormat.class);
				} else {
					System.err.println(sl.getSampleName() + " has no fq1!");
					System.exit(1);
				}
				if (sl.getFastq2() != null) {
					MultipleInputs.addInputPath(job, new Path(sl.getFastq2()),
							FastqInputFormat.class);
				} else {
					System.err.println(sl.getSampleName() + " is SE data!");
				}
				if (sl.getAdapter1() != null) {
					MultipleInputs.addInputPath(job,
							new Path(sl.getAdapter1()),
							AdaptorInputFormat.class);
				}
				if (sl.getAdapter2() != null) {
					MultipleInputs.addInputPath(job,
							new Path(sl.getAdapter2()),
							AdaptorInputFormat.class);
				}
			}
		} else {
			if (option.getInputFastq1() != null) {
				MultipleInputs.addInputPath(job,
						new Path(option.getInputFastq1()),
						FastqInputFormat.class);
			}
			if (option.getInputFastq2() != null) {
				MultipleInputs.addInputPath(job,
						new Path(option.getInputFastq2()),
						FastqInputFormat.class);
			}
			if (option.getAdapter1() != null) {
				MultipleInputs.addInputPath(job,
						new Path(option.getAdapter1()),
						AdaptorInputFormat.class);
			}
			if (option.getAdapter2() != null) {
				MultipleInputs.addInputPath(job,
						new Path(option.getAdapter2()),
						AdaptorInputFormat.class);
			}
		}

		Path outputPath = new Path(option.getOutputDirectory() + "/out_fq");
		FileOutputFormat.setOutputPath(job, outputPath);
		MultipleOutputs.addNamedOutput(job, "filterStatistic",
				TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "qualFreqStatistic",
				TextOutputFormat.class, NullWritable.class, Text.class);

		if (job.waitForCompletion(true)) {
			FastqQualityControlReporterIO report = new FastqQualityControlReporterIO(
					sample, option.isMultiStatis());
			report.mergeReport(outputPath, conf,
					new Path(option.getOutputDirectory()));
			return 0;
		} else {
			return 1;
		}
	}
}
