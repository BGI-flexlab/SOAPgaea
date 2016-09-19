package org.bgi.flexlab.gaea.inputformat.cram;

import htsjdk.samtools.util.Log;
import htsjdk.samtools.util.Log.LogLevel;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

public class GaeaCramInputFormat extends
		FileInputFormat<LongWritable, SAMRecordWritable> {

	@Override
	public RecordReader<LongWritable, SAMRecordWritable> createRecordReader(
			InputSplit inputSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		Log.setGlobalLogLevel(LogLevel.ERROR);
		Configuration conf = context.getConfiguration();
		int type = conf.getInt("cram.inputformat.type", 0);
		CramInputFormatType inputType = CramInputFormatType.valueOf(type);
		
		RecordReader<LongWritable, SAMRecordWritable> rr = null;
		if(inputType == CramInputFormatType.ALL)
			rr = new GaeaCramRecordReader();
		else if(inputType == CramInputFormatType.CHROMOSOME)
			rr = new GaeaCramChromosomeRecordReader();
		return rr;
	}
	
	public boolean isSplitable(JobContext job, Path path) {
		return false;
	}
}
