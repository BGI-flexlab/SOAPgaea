package org.bgi.flexlab.gaea.tools.mapreduce.vcf.report;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.bgi.flexlab.gaea.data.mapreduce.input.vcf.VCFMultipleInputFormat;
import org.bgi.flexlab.gaea.data.structure.header.MultipleVCFHeader;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.BioJob;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.ToolsRunner;
import org.seqdoop.hadoop_bam.VariantContextWritable;

public class VCFQCReport extends ToolsRunner{

	private VCFQCReportOptions options;
	private MultipleVCFHeader vcfHeaders;
    
	public VCFQCReport() {
		this.toolsDescription = "Gaea VCFQC report\n"
				+ "The purpose of this module is to attain the statistics information"
				+ "for the input vcf file.";
	}

	@Override
	public int run(String[] args) throws Exception {
		BioJob job = BioJob.getInstance();
		Configuration conf = job.getConfiguration();
		
		options = new VCFQCReportOptions();
		options.parse(args);
		options.setHadoopConf(args, conf);
		
		//merge header
		vcfHeaders = new MultipleVCFHeader();
		vcfHeaders.mergeHeader(new Path(options.getInputs()), options.getOutputPath(), job, false);
				
		job.setJarByClass(VCFQCReport.class);
		job.setMapperClass(VCFQCReportMapper.class);
		job.setReducerClass(VCFQCReportReducer.class);
		job.setOutputKeyValue(IntWritable.class, Text.class, 
				NullWritable.class, Text.class);
		job.setNumReduceTasks(vcfHeaders.getFileNum());
		
		FileInputFormat.addInputPaths(job, options.getInputs());
		job.setInputFormatClass(VCFMultipleInputFormat.class);

		Path statictisOutput = new Path(options.getOutputPath());
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, statictisOutput);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
}

final class VCFQCReportMapper extends Mapper<LongWritable, VariantContextWritable, IntWritable, Text>{
	
	@Override
	public void map(LongWritable key, VariantContextWritable value, Context context) throws IOException, InterruptedException {
		int fileID = (int) key.get();
		VariantInformation vi = new VariantInformation.Builder(value.get()).isSnp().isTransition().isIndel().build();
		context.write(new IntWritable(fileID), new Text(vi.toString()));
	}
}

final class VCFQCReportReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Iterator<Text> iterator = values.iterator();
		VariantInformation sum = new VariantInformation.Builder()
				.buildFrom(iterator.next().toString());
		for(Text value : values) {
			VariantInformation sub = new VariantInformation.Builder().buildFrom(value.toString());
			sum.combine(sub);
		}
		context.write(NullWritable.get(), new Text(sum.formatTable()));
	}
}