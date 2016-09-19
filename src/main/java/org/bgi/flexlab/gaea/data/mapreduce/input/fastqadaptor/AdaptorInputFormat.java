package org.bgi.flexlab.gaea.data.mapreduce.input.fastqadaptor;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.bgi.flexlab.gaea.data.mapreduce.input.fastq.FastqRecordReader;

public class AdaptorInputFormat extends FileInputFormat<Text, Text> {
	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		Configuration configuration = context.getConfiguration();
		int readNameType = configuration.getInt(
				FastqRecordReader.READ_NAME_TYPE, 0);
		AdaptorRecordReader adapterReader = null;
		if (readNameType != 2) {
			adapterReader = new AdaptorRecordReader();
		} else {
			adapterReader = new AdaptorSpecialRecordReader();
		}
		adapterReader.initialize(split, context);
		return adapterReader;
	}

	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		CompressionCodec codec = new CompressionCodecFactory(
				context.getConfiguration()).getCodec(file);
		return codec == null;
	}
}
