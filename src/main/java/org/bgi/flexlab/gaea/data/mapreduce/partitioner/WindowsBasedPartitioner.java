package org.bgi.flexlab.gaea.data.mapreduce.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.bgi.flexlab.gaea.data.mapreduce.writable.WindowsBasedWritable;

public class WindowsBasedPartitioner<T> extends Partitioner<WindowsBasedWritable, T> {

	@Override
	public int getPartition(WindowsBasedWritable key, T v, int numPartitioner) {
		LongWritable windowsInfo = key.getWindowsInformation();
		int hashcode = windowsInfo.hashCode() / 163 ;
		return Math.abs(hashcode) % numPartitioner;
	}
}
