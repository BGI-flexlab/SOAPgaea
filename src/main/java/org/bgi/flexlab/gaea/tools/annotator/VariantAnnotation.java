package org.bgi.flexlab.gaea.tools.annotator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.bgi.flexlab.gaea.data.structure.reference.ReferenceShare;

public class VariantAnnotation extends Configured implements Tool{
	
	/**
	 * 参数
	 */
	
	Parameter parameter=null;
	
	public VariantAnnotation(){}
	
	public VariantAnnotation(Parameter parameter)
	{
		this.parameter=parameter;
	}
	
	/**
	 * 利用Parameter对象初始化Configuration对象
	 * 
	 * @param conf
	 * @param parameter
	 */
	private static void setConfiguration(Configuration conf, Parameter parameter) {
		
		//set reference
		conf.set("inputFilePath", parameter.getInputFilePath());
		conf.set("reference", parameter.getReferenceSequencePath());
		conf.set("configFile", parameter.getConfigFile());
		conf.set("outputType", parameter.getOutputType());
		conf.setBoolean("verbose", parameter.isVerbose());
		conf.setBoolean("debug", parameter.isDebug());
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = new Configuration();
		parameter = new Parameter(arg0);
		setConfiguration(conf, parameter);
		
		Job job = Job.getInstance(conf, "GaeaAnnotator");
		job.setNumReduceTasks(0);
		job.setJarByClass(VariantAnnotation.class);
		job.setMapperClass(VariantAnnotationMapper.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(MNLineInputFormat.class);
		
		MNLineInputFormat.addInputPath(job, new Path(parameter.getInputFilePath()));
		MNLineInputFormat.setMinNumLinesToSplit(job,1000); //按行处理的最小单位
		MNLineInputFormat.setMapperNum(job, parameter.getMapperNum()); 
		FileOutputFormat.setOutputPath(job, new Path(parameter.getOutputPath()));
		if (job.waitForCompletion(true)) {
//     TODO	 
			return 0;
		}else {
			return 1;
		}
//		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
