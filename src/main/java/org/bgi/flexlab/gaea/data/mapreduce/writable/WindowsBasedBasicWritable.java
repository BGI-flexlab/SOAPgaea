package org.bgi.flexlab.gaea.data.mapreduce.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class WindowsBasedBasicWritable implements WritableComparable<WindowsBasedBasicWritable> {
	protected Text windowsInfo = new Text();

	public void set(String sample, String chromosome, int winNum, int pos) {
		set(sample + ":" + chromosome + ":" + winNum, pos);
	}

	public void set(String chromosome, int winNum, int pos) {
		set(chromosome + ":" + winNum, pos);
	}

	public void set(String winInfo, int pos) {
		this.windowsInfo.set(winInfo);
	}

	public String toString() {
		return windowsInfo.toString();
	}

	public String getChromosomeName() {
		String[] win = windowsInfo.toString().split(":");
		return win[win.length - 2];
	}

	public Text getWindows() {
		return windowsInfo;
	}

	public String getWindowsInformation() {
		return windowsInfo.toString();
	}

	public int getWindowsNumber() {
		String[] win = windowsInfo.toString().split(":");
		return Integer.parseInt(win[win.length - 1]);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		windowsInfo.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		windowsInfo.write(out);
	}

	@Override
	public int hashCode() {
		return windowsInfo.hashCode() * 163;
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof WindowsBasedBasicWritable) {
			WindowsBasedBasicWritable tmp = (WindowsBasedBasicWritable) other;
			return windowsInfo.toString().equals(tmp.getWindowsInformation());
		}
		return false;
	}

	@Override
	public int compareTo(WindowsBasedBasicWritable tp) {
		return  windowsInfo.compareTo(tp.getWindows());
	}
}
