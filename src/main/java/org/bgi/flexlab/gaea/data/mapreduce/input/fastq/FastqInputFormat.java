package org.bgi.flexlab.gaea.data.mapreduce.input.fastq;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class FastqInputFormat  extends FileInputFormat<Text, Text>{
	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException, InterruptedException {
		return new FastqRecordReader();
	}

	@Override
	protected boolean isSplitable(JobContext context, Path file){
		if (file.toString().endsWith(".gz"))
			return false;
		return true;
	}
}
