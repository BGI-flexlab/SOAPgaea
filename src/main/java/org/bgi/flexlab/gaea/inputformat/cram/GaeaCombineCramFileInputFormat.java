package org.bgi.flexlab.gaea.inputformat.cram;

import htsjdk.samtools.util.Log;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

public class GaeaCombineCramFileInputFormat extends
		CombineFileInputFormat<LongWritable, SAMRecordWritable> {

	@Override
	public RecordReader<LongWritable, SAMRecordWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		Log.setGlobalLogLevel(Log.LogLevel.ERROR);
		RecordReader<LongWritable, SAMRecordWritable> rr = new GaeaCombineCramFileRecordReader(
				split, context);
		return rr;
	}

	@Override
	public boolean isSplitable(JobContext job, Path path) {
		return false;
	}
}
