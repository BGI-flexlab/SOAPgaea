/*******************************************************************************
 * Copyright (c) 2017, BGI-Shenzhen
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 *******************************************************************************/
package org.bgi.flexlab.gaea.data.mapreduce.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.bgi.flexlab.gaea.data.exception.OutOfBoundException;

import htsjdk.samtools.SAMRecord;

public class WindowsBasedWritable implements WritableComparable<WindowsBasedWritable> {
	private LongWritable windowsInfo = new LongWritable();
	private IntWritable position = new IntWritable();

	private final static int SAMPLE_BITS = 22;
	private final static int WINDOW_NUMBER_BITS = 32;
	private final static int CHROMOSOME_BITS = Long.BYTES * Byte.SIZE - SAMPLE_BITS - WINDOW_NUMBER_BITS;

	private final static int MAX_SAMPLE_ID = (int) (Math.pow(2, SAMPLE_BITS));
	private final static int CHROMOSOME_BIT_INDEX = SAMPLE_BITS + WINDOW_NUMBER_BITS;

	private final static int CHROMOSOME_BITS_MASK = (int) (Math.pow(2, CHROMOSOME_BITS) - 1);
	private final static int SAMPLE_BITS_MASK = (int) (Math.pow(2, SAMPLE_BITS) - 1);
	private final static int WINDOW_NUMBER_MASK = (int) (Math.pow(2, WINDOW_NUMBER_BITS) - 1);

	public void set(long sample, long chromosome, long winNum, int pos) {
		if (sample >= MAX_SAMPLE_ID)
			throw new OutOfBoundException(String.format("sample size %d is more than 4194304", (int) sample));

		if (winNum >= Integer.MAX_VALUE)
			throw new OutOfBoundException(
					String.format("window number %d is more than %d", (int) winNum, Integer.MAX_VALUE));

		if (chromosome >= CHROMOSOME_BITS_MASK)
			throw new OutOfBoundException(
					String.format("chromosome size %d is more than %d", (int) chromosome, CHROMOSOME_BITS_MASK));

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
		int index = (int) ((key >> CHROMOSOME_BIT_INDEX) & CHROMOSOME_BITS_MASK);
		if (index == CHROMOSOME_BITS_MASK)
			return SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX;
		return index;
	}

	public long getWindows() {
		return windowsInfo.get();
	}

	public LongWritable getWindowsInformation() {
		return windowsInfo;
	}

	public int getWindowsNumber() {
		long key = windowsInfo.get();
		return (int) ((key >> SAMPLE_BITS) & WINDOW_NUMBER_MASK);
	}

	public int getSampleID() {
		long key = windowsInfo.get();
		return (int) (key & SAMPLE_BITS_MASK);
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

	public int partition() {
		long hashcode = (getChromosomeIndex() + 1) * 163;
		hashcode += (getWindowsNumber() + 1) * 127;
		hashcode += (getSampleID() + 1) * 163;
		
		if(hashcode >= (long)(Integer.MAX_VALUE))
			hashcode -= (long)(Integer.MAX_VALUE);
		return (int)(hashcode & 0xffffffff);
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
			if (cmp > 0)
				return 1;
			return -1;
		}
		return position.compareTo(tp.position);
	}
}
