package org.bgi.flexlab.gaea.data.mapreduce.output.bam;

import htsjdk.samtools.util.Log;
import htsjdk.samtools.util.Log.LogLevel;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

public class GaeaBamOutputFormat<K> extends FileOutputFormat<K,SAMRecordWritable> {
	private boolean writeHeader = false;
	
	@Override
	public RecordWriter<K, SAMRecordWritable> getRecordWriter(
			TaskAttemptContext context) throws IOException, InterruptedException {
		return getRecordWriter(context, getDefaultWorkFile(context, ""));
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public RecordWriter<K, SAMRecordWritable> getRecordWriter(
			TaskAttemptContext context, Path outputPath) throws IOException {
		Log.setGlobalLogLevel(LogLevel.ERROR);
		return new GaeaKeyIgnoringBamRecordWriter(outputPath,
				Boolean.valueOf(this.writeHeader), context);
	}

	public void setHeader(boolean isWriterHeader){
		this.writeHeader = isWriterHeader;
	}
}
