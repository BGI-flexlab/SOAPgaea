package org.bgi.flexlab.gaea.tools.mapreduce.variantrecalibratioin;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.bgi.flexlab.gaea.data.mapreduce.input.vcf.VCFMultipleInputFormat;
import org.bgi.flexlab.gaea.data.structure.header.MultipleVCFHeader;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.BioJob;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.ToolsRunner;
import org.bgi.flexlab.gaea.tools.variantrecalibratioin.VCFRecalibrator;
import org.seqdoop.hadoop_bam.KeyIgnoringVCFOutputFormat;
import org.seqdoop.hadoop_bam.VariantContextWritable;

import hbparquet.hadoop.util.ContextUtil;


public class VariantRecalibration extends ToolsRunner{
	public static final String VQS_LOD_KEY = "VQSLOD"; // Log odds ratio of being a true variant versus being false under the trained gaussian mixture model
    public static final String CULPRIT_KEY = "culprit"; // The annotation which was the worst performing in the Gaussian mixture model, likely the reason why the variant was filtered out
    
    public VariantRecalibration() {
		this.toolsDescription = "Gaea variant quality score recalibration\n"
				+ "The purpose of variant recalibration is to assign a well-calibrated "
				+ "probability to each variant call in a call set.";
	}
    
    private VariantRecalibrationOptions options;
    private MultipleVCFHeader vcfHeaders;

	@Override
	public int run(String[] args) throws Exception {
		BioJob job = BioJob.getInstance();
		Configuration conf = job.getConfiguration();
		
		options = new VariantRecalibrationOptions();
		options.parse(args);
		options.setHadoopConf(args, conf);
		
		//merge header
		vcfHeaders = new MultipleVCFHeader();
		vcfHeaders.mergeHeader(new Path(options.getInputs()), options.getOutputPath(), job, false);
				
		job.setJarByClass(VariantRecalibration.class);
		job.setMapperClass(VariantRecalibrationMapper.class);
		job.setReducerClass(VariantRecalibrationReducer.class);
		job.setOutputKeyValue(IntWritable.class, Text.class, 
				NullWritable.class, VariantContextWritable.class);
		job.setNumReduceTasks(vcfHeaders.getFileNum());
		
		FileInputFormat.addInputPaths(job, options.getInputs());
		job.setInputFormatClass(VCFMultipleInputFormat.class);

		Path statictisOutput = new Path(options.getOutputPath() + "/tmp");
		job.setOutputFormatClass(KeyIgnoringVCFOutputFormat.class);
		FileOutputFormat.setOutputPath(job, statictisOutput);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	
}

final class VariantRecalibrationOutputFormat<K> extends FileOutputFormat<K, VariantContextWritable>{
	
	private KeyIgnoringVCFOutputFormat<K> baseOF;
	
	private void initBaseOF(Configuration conf) {
		if (baseOF == null)
			baseOF = new KeyIgnoringVCFOutputFormat<K>(conf);
	}

	@Override public RecordWriter<K,VariantContextWritable> getRecordWriter(
			TaskAttemptContext context)
		throws IOException {
		final Configuration conf = ContextUtil.getConfiguration(context);
		initBaseOF(conf);
		baseOF.setHeader(VCFRecalibrator.finalHeader);
	
		return baseOF.getRecordWriter(context, getDefaultWorkFile(context, ""));
	}

	// Allow the output directory to exist.
	@Override public void checkOutputSpecs(JobContext job) {}
}