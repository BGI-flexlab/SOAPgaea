package org.bgi.flexlab.gaea.data.mapreduce.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.bgi.flexlab.gaea.exception.OutOfBoundException;

public class WindowsBasedWritable implements WritableComparable<WindowsBasedWritable> {
	private LongWritable windowsInfo = new LongWritable();
	private IntWritable position = new IntWritable();
	
	private final static int SAMPLE_BITS = 22;
	private final static int WINDOW_NUMBER_BITS = 32;
	private final static int CHROMOSOME_BITS = Long.BYTES * Byte.SIZE - SAMPLE_BITS - WINDOW_NUMBER_BITS;
	
	private final static int MAX_SAMPLE_ID = (int)(Math.pow(2, SAMPLE_BITS));
	private final static int CHROMOSOME_BIT_INDEX = SAMPLE_BITS + WINDOW_NUMBER_BITS;
	
	private final static int CHROMOSOME_BITS_MASK = (int)(Math.pow(2, CHROMOSOME_BITS) - 1);
	private final static int SAMPLE_BITS_MASK = (int)(Math.pow(2, SAMPLE_BITS) - 1);
	private final static int WINDOW_NUMBER_MASK = (int)(Math.pow(2, WINDOW_NUMBER_BITS) - 1);

	public void set(long sample, long chromosome, long winNum, int pos) {
		if(sample >= MAX_SAMPLE_ID)
			throw new OutOfBoundException(String.format("sample size %d is more than 4194304",(int)sample));
		
		if(winNum >= Integer.MAX_VALUE)
			throw new OutOfBoundException(String.format("window number %d is more than %d",(int)winNum,Integer.MAX_VALUE));
		
		if(chromosome >= CHROMOSOME_BITS_MASK)
			throw new OutOfBoundException(String.format("chromosome size %d is more than %d",(int)chromosome,CHROMOSOME_BITS_MASK));
		
		long key = 0;
		key = (chromosome << CHROMOSOME_BIT_INDEX) | (winNum << SAMPLE_BITS) | sample;
		windowsInfo.set(key);
		position.set(pos);
	}

	public void set(int chromosome, int winNum, int pos) {
		set(0, chromosome, winNum, pos);
	}

	public String toString() {
		return windowsInfo.toString() + "\t" + position.get();
	}

	public int getChromosomeIndex() {
		long key = windowsInfo.get();
		return (int)((key >> CHROMOSOME_BIT_INDEX) & CHROMOSOME_BITS_MASK);
	}

	public long getWindows() {
		return windowsInfo.get();
	}

	public LongWritable getWindowsInformation() {
		return windowsInfo;
	}

	public int getWindowsNumber() {
		long key = windowsInfo.get();
		return (int)((key >> SAMPLE_BITS) & WINDOW_NUMBER_MASK);
	}
	
	public int getSampleID(){
		long key = windowsInfo.get();
		return (int)(key & SAMPLE_BITS_MASK);
	}

	public IntWritable getPosition() {
		return position;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		windowsInfo.readFields(in);
		position.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		windowsInfo.write(out);
		position.write(out);
	}

	@Override
	public int hashCode() {
		return windowsInfo.hashCode() * 163 + position.hashCode();
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof WindowsBasedWritable) {
			WindowsBasedWritable tmp = (WindowsBasedWritable) other;
			return windowsInfo.toString().equals(tmp.getWindowsInformation())
					&& position.get() == (tmp.getPosition().get());
		}
		return false;
	}

	@Override
	public int compareTo(WindowsBasedWritable tp) {
		long cmp = windowsInfo.get() - tp.getWindows();
		if (cmp != 0) {
			return (int) (cmp & 0xffffffff);
		}
		return position.compareTo(tp.position);
	}
}
