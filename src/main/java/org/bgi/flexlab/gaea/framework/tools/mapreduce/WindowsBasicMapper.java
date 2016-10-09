package org.bgi.flexlab.gaea.framework.tools.mapreduce;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.bgi.flexlab.gaea.data.mapreduce.input.header.SamFileHeader;
import org.bgi.flexlab.gaea.data.mapreduce.writable.WindowsBasicWritable;
import org.bgi.flexlab.gaea.exception.FileNotExistException;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

public class WindowsBasicMapper
		extends
		Mapper<LongWritable, SAMRecordWritable, WindowsBasicWritable, SAMRecordWritable> {

	public final static String WINDOWS_SIZE = "windows.size";
	public final static String WINDOWS_EXTEND_SIZE = "windows.extend.size";
	public final static String MULTIPLE_SAMPLE = "multiple.sample";

	protected int windowsSize;
	protected int windowsExtendSize;
	protected boolean multiSample;
	protected SAMFileHeader header = null;

	protected WindowsBasicWritable keyout = new WindowsBasicWritable();

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		windowsSize = conf.getInt(WINDOWS_SIZE, 10000);
		windowsExtendSize = conf.getInt(WINDOWS_EXTEND_SIZE, 500);
		multiSample = conf.getBoolean(MULTIPLE_SAMPLE, false);
		
		header = SamFileHeader.getHeader(conf);
		
		if(header == null){
			String[] filename=context.getInputSplit().toString().split("/|:");
			throw new FileNotExistException.MissingHeaderException(filename[filename.length-2]);
		}
	}

	protected boolean filter(SAMRecord sam) {
		return false;
	}

	protected int[] getExtendPosition(int start, int end, int length) {
		int[] winNum = new int[3];

		winNum[1] = (int) ((start - windowsExtendSize) > 0 ? (start - windowsExtendSize)
				: 0)
				/ windowsSize;
		winNum[0] = start / windowsSize;
		winNum[2] = (int) ((end + windowsExtendSize) > length ? (end + windowsExtendSize)
				: length)
				/ windowsSize;

		return winNum;
	}

	protected void setKey(SAMRecord sam, int winNum) {
		if (multiSample) {
			keyout.set(sam.getReadGroup().getSample(), sam.getReferenceName(),
					winNum, sam.getAlignmentStart());
		} else {
			keyout.set(sam.getReferenceName(), winNum, sam.getAlignmentStart());
		}
	}

	@Override
	protected void map(LongWritable key, SAMRecordWritable value,
			Context context) throws IOException, InterruptedException {
		SAMRecord sam = value.get();
		sam.setHeader(header);
		if (filter(sam)) {
			return;
		}

		String chrName = sam.getReferenceName();
		int[] winNums = getExtendPosition(sam.getAlignmentStart(),
				sam.getAlignmentEnd(), header.getSequence(chrName)
						.getSequenceLength());
		for (int i = 0; i < 3; i++) {
			if (i != 0 && winNums[i] == winNums[0]) {
				continue;
			}
			setKey(sam, winNums[i]);
			context.write(keyout, value);
		}
	}
}
