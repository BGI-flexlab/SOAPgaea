package org.bgi.flexlab.gaea.data.mapreduce.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class WindowsBasicWritable implements
		WritableComparable<WindowsBasicWritable> {
	private Text windowsInfo = new Text();
	private IntWritable position = new IntWritable();

	public void set(String sample, String chromosome, int winNum,
			int pos) {
		set(sample+":"+chromosome+":"+winNum,pos);
	}
	
	public void set(String chromosome, int winNum,
			int pos) {
		set(chromosome+":"+winNum,pos);
	}
	
	protected void set(String winInfo,int pos){
		windowsInfo.set(winInfo);
		position.set(pos);
	}

	public String toString() {
		return windowsInfo.toString() + "\t" + position;
	}

	public String getChromosomeName() {
		String[] win = windowsInfo.toString().split(":");
		return win[win.length - 2];
	}
	
	public String getWindowsInformation(){
		return windowsInfo.toString();
	}

	public int getWindowsNumber() {
		String[] win = windowsInfo.toString().split(":");
		return Integer.parseInt(win[win.length - 1]);
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
		if (other instanceof WindowsBasicWritable) {
			WindowsBasicWritable tmp = (WindowsBasicWritable) other;
			return windowsInfo.toString().equals(tmp.windowsInfo.toString())
					&& position.get() == (tmp.position.get());
		}
		return false;
	}

	@Override
	public int compareTo(WindowsBasicWritable tp) {
		int cmp = windowsInfo.compareTo(tp.windowsInfo);
		if (cmp != 0) {
			return cmp;
		}
		return position.compareTo(tp.position);
	}
}
