package org.bgi.flexlab.gaea.data.mapreduce.input.vcf;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.seqdoop.hadoop_bam.VariantContextWritable;

public class VCFMultipleInputFormat extends FileInputFormat<LongWritable, VariantContextWritable> {

	@Override
	public RecordReader<LongWritable, VariantContextWritable> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException
	{
		return new VCFRecordReader(context.getConfiguration(), false);
	}

	@Override
	protected boolean isSplitable(JobContext context, Path file)
	{
		CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
		return codec == null;
	}


}
