package org.bgi.flexlab.gaea.data.mapreduce.input.sam;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.bgi.flexlab.gaea.data.mapreduce.writable.SamRecordWritable;

public class GaeaSamInputFormat extends FileInputFormat<LongWritable, SamRecordWritable> {
	@Override
	public RecordReader<LongWritable, SamRecordWritable> createRecordReader(InputSplit split, TaskAttemptContext ctx)
			throws InterruptedException, IOException {
		final RecordReader<LongWritable, SamRecordWritable> rr = new GaeaSamRecordReader();
		rr.initialize(split, ctx);
		return rr;
	}

	@Override
	public boolean isSplitable(JobContext job, Path path) {
		return super.isSplitable(job, path);
	}
}
