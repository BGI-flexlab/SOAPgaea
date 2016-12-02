package org.bgi.flexlab.gaea.data.mapreduce.partitioner;

import java.io.IOException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.bgi.flexlab.gaea.data.mapreduce.writable.WindowsBasedBasicWritable;

public class WindowsBasedBasicSort implements RawComparator<WindowsBasedBasicWritable> {

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		WindowsBasedBasicWritable key1 = new WindowsBasedBasicWritable();
		WindowsBasedBasicWritable key2 = new WindowsBasedBasicWritable();
		DataInputBuffer buffer = new DataInputBuffer();
		try {
			buffer.reset(b1, s1, l1);
			key1.readFields(buffer);
			buffer.reset(b2, s2, l2);
			key2.readFields(buffer);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		return key1.compareTo(key2);
	}

	@Override
	public int compare(WindowsBasedBasicWritable o1, WindowsBasedBasicWritable o2) {
		return o1.getWindows().compareTo(o2.getWindows());
	}
}
