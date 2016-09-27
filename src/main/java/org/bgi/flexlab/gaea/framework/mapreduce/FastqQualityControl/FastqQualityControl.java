package org.bgi.flexlab.gaea.framework.mapreduce.FastqQualityControl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.bgi.flexlab.gaea.framework.mapreduce.ToolsRunner;
import org.bgi.flexlab.gaea.options.FastqQualityControlOptions;

public class FastqQualityControl extends ToolsRunner {

	public FastqQualityControl() {
		this.toolsDescription = "";
	}

	@Override
	public int run(String[] args) throws Exception {
		FastqQualityControlOptions option = new FastqQualityControlOptions();
		option.parse(args);

		Configuration conf = new Configuration();
		option.setHadoopConf(args, conf);

		Job job = Job.getInstance();
		job.setJobName("GaeaFastqQC");
		job.setJarByClass(FastqQualityControl.class);
		job.setMapperClass(FastqQualityControlMapper.class);
		job.setReducerClass(FastqQualityControlReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(option.getReducerNumber());

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
