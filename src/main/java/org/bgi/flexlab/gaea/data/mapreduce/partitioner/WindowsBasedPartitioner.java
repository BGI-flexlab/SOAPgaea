package org.bgi.flexlab.gaea.data.mapreduce.partitioner;

import org.apache.hadoop.mapreduce.Partitioner;
import org.bgi.flexlab.gaea.data.mapreduce.writable.WindowsBasedWritable;

public class WindowsBasedPartitioner<T> extends Partitioner<WindowsBasedWritable, T> {

	@Override
	public int getPartition(WindowsBasedWritable key, T v, int numPartitioner) {
		int hashcode = key.partition() ;
		return Math.abs(hashcode) % numPartitioner;
	}
}
