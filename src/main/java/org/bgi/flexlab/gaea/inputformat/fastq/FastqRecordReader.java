package org.bgi.flexlab.gaea.inputformat.fastq;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class FastqRecordReader extends RecordReader<Text, Text> {
	public final static String READ_NAME_TYPE = "read.name.type";

	private FastqBasicReader reader = null;
	private Text key = null;
	private Text value = null;

	public FastqRecordReader() throws IOException {
	}

	@Override
	public void close() throws IOException {
		reader.close();
	}

	@Override
	public float getProgress() throws IOException {
		return reader.getProgress();
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		Configuration configuration = context.getConfiguration();
		int readNameType = configuration.getInt(READ_NAME_TYPE, 0);
		byte[] recordDelimiter = configuration.get(
				"textinputformat.record.delimiter").getBytes();
		if (readNameType == 0) {// read id format : reads_XX/1
			reader = new FastqForwardSlashReader(configuration,
					(FileSplit) split, recordDelimiter);
		} else if (readNameType == 1) {// read id format : reads_xx: 1:N:XX
										// reads_xx: 2:N:XX
			reader = new FastqSapceReader(configuration, (FileSplit) split,
					recordDelimiter);
		} else if (readNameType == 2) {// read id format : reads_xx
			reader = new FastqSpecialReader(configuration, (FileSplit) split,
					recordDelimiter);
		}
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return reader.next(key, value);
	}
}
