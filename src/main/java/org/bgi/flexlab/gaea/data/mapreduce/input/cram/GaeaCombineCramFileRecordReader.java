package org.bgi.flexlab.gaea.data.mapreduce.input.cram;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

public class GaeaCombineCramFileRecordReader extends
		RecordReader<LongWritable, SAMRecordWritable> {
	protected GaeaCramRecordReader currentReader = null;
	protected CombineFileSplit split;
	protected int fileIndex;
	protected TaskAttemptContext context;
	
	public GaeaCombineCramFileRecordReader(InputSplit split, TaskAttemptContext context){
		this.split = (CombineFileSplit)split;
		this.context = context;
		this.fileIndex = 0;
		
		System.err.println("total number of file is "+this.split.getNumPaths());
		try {
			initializeNextRecordReader();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void close() throws IOException {
		if (currentReader != null) {
			currentReader.close();
			currentReader = null;
		}
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		return currentReader.getCurrentKey();
	}

	@Override
	public SAMRecordWritable getCurrentValue() throws IOException,
			InterruptedException {
		return currentReader.getCurrentValue();
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		int subprogress = 0;
		if(fileIndex != 0)
			subprogress = fileIndex - 1;
		return (float) (subprogress) / split.getNumPaths();
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		if(null == currentReader){
			initializeNextRecordReader();
		}
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		while ((currentReader == null) || !currentReader.nextKeyValue()) {
			if (!initializeNextRecordReader()) {
				return false;
			}
		}
		return true;
	}

	protected boolean initializeNextRecordReader() throws IOException {
		if (currentReader != null) {
			currentReader.close();
			currentReader = null;
		}

		// if all chunks have been processed, nothing more to do.
		if (fileIndex == split.getNumPaths()) {
			return false;
		}

		// get a record reader for the fileIndex-th chunk
		try {
			Configuration conf = context.getConfiguration();
		    
			currentReader = new GaeaCramRecordReader();
			
			Path path = split.getPath(fileIndex);
			long length = split.getLength(fileIndex);
			FileSystem fs = path.getFileSystem(conf);
			FileStatus status = fs.getFileStatus(path);
			BlockLocation[] blkLocations = fs.getFileBlockLocations(status, 0, length);
			
			currentReader.initialize(new FileSplit(path, 0, length, blkLocations[0].getHosts()), context);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		fileIndex++;
		return true;
	}
}
