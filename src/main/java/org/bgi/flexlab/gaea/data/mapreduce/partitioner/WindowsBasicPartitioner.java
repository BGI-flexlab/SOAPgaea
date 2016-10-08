package org.bgi.flexlab.gaea.data.mapreduce.partitioner;

import org.apache.hadoop.mapreduce.Partitioner;
import org.bgi.flexlab.gaea.data.mapreduce.writable.WindowsBasicWritable;

public class WindowsBasicPartitioner<T> extends Partitioner<WindowsBasicWritable, T> {
	
	@Override
	public int getPartition(WindowsBasicWritable key, T v, int numPartitioner) {
		String windowsInfo = key.getWindowsInformation();
		String[] array = windowsInfo.split(":");
		int len = array.length;
		int value = Integer.parseInt(array[len-1]);
		for(int i=len-1;i>0;i--)
		{
			value =value*127+array[i-1].hashCode();
		}
		
		return Math.abs(value)%numPartitioner;
	}
}
