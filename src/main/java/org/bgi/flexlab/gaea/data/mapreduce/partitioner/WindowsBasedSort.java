package org.bgi.flexlab.gaea.data.mapreduce.partitioner;

import java.io.IOException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.bgi.flexlab.gaea.data.mapreduce.writable.WindowsBasedWritable;

public class WindowsBasedSort implements RawComparator<WindowsBasedWritable> {
	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		WindowsBasedWritable key1 = new WindowsBasedWritable();
		WindowsBasedWritable key2 = new WindowsBasedWritable();
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
	public int compare(WindowsBasedWritable o1, WindowsBasedWritable o2) {
		int ii = o1.getWindows().compareTo(o2.getWindows());
		if (ii != 0) {
			return ii;
		} else {
			return o1.getPosition().compareTo(o2.getPosition());
		}
	}
}
