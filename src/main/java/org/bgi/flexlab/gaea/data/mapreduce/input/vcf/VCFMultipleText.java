package org.bgi.flexlab.gaea.data.mapreduce.input.vcf;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class VCFMultipleText extends FileInputFormat<IntWritable, Text> {

	@Override
	public RecordReader<IntWritable, Text> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException
	{

		String delimiter = context.getConfiguration().get(
		        "textinputformat.record.delimiter");
		    byte[] recordDelimiterBytes = null;
		    if (null != delimiter)
		      recordDelimiterBytes = delimiter.getBytes();
		    return new VCFMultipleReader(recordDelimiterBytes);
	}

	@Override
	protected boolean isSplitable(JobContext context, Path file)
	{
		CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
		return codec == null;
	}


}
