package org.bgi.flexlab.gaea.tools.mapreduce.FastqQualityControl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.bgi.flexlab.gaea.data.mapreduce.input.fastq.FastqInputFormat;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.BioJob;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.PairEndAggregatorMapper;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.ToolsRunner;

public class FastqQualityControl extends ToolsRunner {

	public FastqQualityControl() {
		this.toolsDescription = "Gaea fastq quality control\n"
				+ "this a simple description";
	}

	@Override
	public int run(String[] args) throws Exception {
		FastqQualityControlOptions option = new FastqQualityControlOptions();
		option.parse(args);

		BioJob job = BioJob.getInstance();
		
		Configuration conf = job.getConfiguration();
		option.setHadoopConf(args, conf);
		
		job.setJobName("GaeaFastqQC");
		job.setJarByClass(FastqQualityControl.class);
		job.setMapperClass(PairEndAggregatorMapper.class);
		job.setReducerClass(FastqQualityControlReducer.class);

		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(option.getReducerNumber());

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
