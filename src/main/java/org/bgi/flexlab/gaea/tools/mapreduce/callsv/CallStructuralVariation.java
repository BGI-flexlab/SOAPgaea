package org.bgi.flexlab.gaea.tools.mapreduce.callsv;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.bgi.flexlab.gaea.data.mapreduce.input.bam.GaeaAnySAMInputFormat;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.BioJob;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.ToolsRunner;
import org.bgi.flexlab.gaea.tools.callsv.Format;
import org.bgi.flexlab.gaea.tools.callsv.NewMapKey;
import org.bgi.flexlab.gaea.tools.callsv.ReduceGroupingComparator;

public class CallStructuralVariation extends ToolsRunner{
	
	public CallStructuralVariation() {
		this.toolsDescription = "Gaea structural variantion calling";
	}

	private CallStructuralVariationOptions options = null;
	
	private int runCallStructuralVariation(String[] args) throws IOException {
		/**
		 * set job1 info
		 */
		BioJob job1 = BioJob.getInstance();
		
		Configuration conf1 = job1.getConfiguration();
		
		String[] remainArgs1 = remainArgs(args, conf1);
		options = new CallStructuralVariationOptions();
		options.parse(remainArgs1);
		options.setHadoopConf(remainArgs1, conf1);
		
		job1.setJobName("CallSV Sort");
		job1.setJarByClass(CallStructuralVariation.class);
		job1.setMapperClass(CallStructuralVariationMapper1.class);
		job1.setReducerClass(CallStructuralVariationReducer1.class);
		job1.setNumReduceTasks(options.getReducenum());
		job1.setInputFormatClass(GaeaAnySAMInputFormat.class);
		job1.setMapOutputKeyClass(NewMapKey.class);
		job1.setMapOutputValueClass(Format.class);
		job1.setOutputKeyClass(NullWritable.class);
		job1.setOutputValueClass(Text.class);
		//job1.setPartitionerClass(ChrPartitionar.class);
		job1.setGroupingComparatorClass(ReduceGroupingComparator.class);

		FileInputFormat.addInputPaths(job1, options.getInput());
		FileOutputFormat.setOutputPath(job1, new Path(options.getHdfsdir() + "/Sort/Result"));
		
		
		/**
		 * set job2 info
		 */
		BioJob job2 = BioJob.getInstance();
		
		Configuration conf2 = job2.getConfiguration();
		
		String[] remainArgs2 = remainArgs(args, conf2);
		options = new CallStructuralVariationOptions();
		options.parse(remainArgs2);
		options.setHadoopConf(remainArgs2, conf2);	
		
		job1.setJobName("CallSV calling");
		job2.setJarByClass(CallStructuralVariation.class);
		job2.setMapperClass(CallStructuralVariationMapper2.class);
		job2.setReducerClass(CallStructuralVariationReducer2.class);
		job2.setNumReduceTasks(options.getReducenum());
		job2.setMapOutputKeyClass(NewMapKey.class);
		job2.setMapOutputValueClass(Format.class);
		job2.setOutputKeyClass(NullWritable.class);
		job2.setOutputValueClass(Text.class);
		//job2.setSortComparatorClass(NewMapKeyComparator.class);
		//job2.setPartitionerClass(ChrPartitionar.class);
		job2.setGroupingComparatorClass(ReduceGroupingComparator.class);

		FileInputFormat.addInputPaths(job2, options.getHdfsdir() + "/Sort/Result");
		FileOutputFormat.setOutputPath(job2, new Path(options.getHdfsdir() + "/Calling/Result"));
		
		/*
		 * set Control job
		 */
		ControlledJob ctrolJob1 = new ControlledJob(conf1);
		ctrolJob1.setJob(job1);
		ControlledJob ctrolJob2 = new ControlledJob(conf2);
		ctrolJob2.setJob(job2);
		
		ctrolJob2.addDependingJob(ctrolJob1);//job2的启动，依赖于job1作业的完成
		
		/*
		 * set main control job
		 */
		JobControl jobCtrl = new JobControl("my Ctrol");
		jobCtrl.addJob(ctrolJob1);
		jobCtrl.addJob(ctrolJob2);
		
		/*
		 * set threads
		 */
		Thread t = new Thread(jobCtrl);
		t.start();
		while(true) {
			if(jobCtrl.allFinished()){//如果作业成功完成，就打印成功作业的信息   
				System.out.println(jobCtrl.getSuccessfulJobList());   
				jobCtrl.stop();  
				return 0;
			}  else {
				return 1;
			}
		}

		
	}
	
	@Override
	public int run(String[] args) throws Exception {
		CallStructuralVariation sv = new CallStructuralVariation();
		int res = sv.runCallStructuralVariation(args);
		return res;
	}

}
