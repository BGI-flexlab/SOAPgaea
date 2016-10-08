package org.bgi.flexlab.gaea.data.mapreduce.partitioner;

import java.io.IOException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.bgi.flexlab.gaea.data.mapreduce.writable.WindowsBasicWritable;

public class WindowsBasicComparator implements
		RawComparator<WindowsBasicWritable> {

	@Override
	public int compare(WindowsBasicWritable o1, WindowsBasicWritable o2) {
		return o1.getWindowsInformation().compareTo(o2.getWindowsInformation());
	}

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		WindowsBasicWritable key1 = new WindowsBasicWritable();
		WindowsBasicWritable key2 = new WindowsBasicWritable();
		DataInputBuffer buffer = new DataInputBuffer();
		try {
			buffer.reset(b1, s1, l1);
			key1.readFields(buffer);
			buffer.reset(b2, s2, l2);
			key2.readFields(buffer);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		return compare(key1, key2);
	}

}
