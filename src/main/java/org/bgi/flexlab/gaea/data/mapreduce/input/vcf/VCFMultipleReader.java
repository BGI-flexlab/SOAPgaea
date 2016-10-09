package org.bgi.flexlab.gaea.data.mapreduce.input.vcf;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.util.LineReader;
import org.bgi.flexlab.gaea.data.structure.header.MultipleVCFHeader;

public class VCFMultipleReader extends RecordReader<IntWritable, Text> {
	private static final Log LOG = LogFactory.getLog(LineRecordReader.class);

	private CompressionCodecFactory compressionCodecs = null;
	private long start;
	private long pos;
	private long end;
	private LineReader in;
	private int maxLineLength;
	private IntWritable key = null;
	private Text value = null;
	private byte[] recordDelimiterBytes;
	private int fileID;

	public VCFMultipleReader() {
	}

	public VCFMultipleReader(byte[] recordDelimiter) {
		this.recordDelimiterBytes = recordDelimiter;
	}

	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException {
		FileSplit split = (FileSplit) genericSplit;
		
		Configuration job = context.getConfiguration();
		
		MultipleVCFHeader vcfHeader = new MultipleVCFHeader();
		vcfHeader.loadVcfHeader(job.get("output"));
		fileID = vcfHeader.getId(split.getPath().toString());

		this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
				Integer.MAX_VALUE);
		
		start = split.getStart();
		end = start + split.getLength();
		
		final Path file = split.getPath();
		compressionCodecs = new CompressionCodecFactory(job);
		final CompressionCodec codec = compressionCodecs.getCodec(file);

		// open the file and seek to the start of the split
		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(split.getPath());
		initPos(codec, fileIn, job);
	}

	@Override
	public boolean nextKeyValue() throws IOException {
		if (key == null) {
			key = new IntWritable();
		}
		if (value == null) {
			value = new Text();
		}
		// System.err.println("next key value");
		int newSize = 0;
		boolean iswrongVcf = false;
		while (pos < end) {
			Text tmp = new Text();
			newSize = in.readLine(tmp, maxLineLength,
					Math.max((int) Math.min(Integer.MAX_VALUE, end - pos),
							maxLineLength));
			if (newSize == 0) {
				iswrongVcf = true;
				break;
			}
			pos += newSize;

			if (tmp.toString().startsWith("#")) {
				continue;
			}
			
			if (!iswrongVcf) {
				// System.err.println(readsNum);
				// System.err.println("start:" + start + "\tend:" + end +
				// "\tpos:" + pos);

				key.set(fileID);
				value.set(tmp);
			} else {
				LOG.warn("wrong vcf file:blank line among fq file or end of file!");
			}
			break;
		}
		if (newSize == 0 || iswrongVcf) {
			key = null;
			value = null;
			return false;
		} else {
			return true;
		}
	}

	@Override
	public IntWritable getCurrentKey() {
		return key;
	}

	@Override
	public Text getCurrentValue() {
		return value;
	}

	/**
	 * Get the progress within the split
	 */
	public float getProgress() {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos - start) / (float) (end - start));
		}
	}

	public synchronized void close() throws IOException {
		if (in != null) {
			in.close();
		}
	}
	
	private void initPos(CompressionCodec codec, FSDataInputStream fileIn, Configuration job) throws IOException{
		boolean skipFirstLine = false;
		if (codec != null) {
			initLineReader(codec.createInputStream(fileIn), job);
			end = Long.MAX_VALUE;
		} else {
			if (start != 0) {
				skipFirstLine = true;
				--start;
				fileIn.seek(start);
			}
			initLineReader(fileIn, job);
		}
		if (skipFirstLine) { // skip first line and re-establish "start".
			start += in.readLine(new Text(), 0,
					(int) Math.min((long) Integer.MAX_VALUE, end - start));
		}
		this.pos = start;
	}
	
	private void initLineReader(InputStream is, Configuration job) throws IOException {
		if (null == this.recordDelimiterBytes) {
			in = new LineReader(is, job);
		} else {
			in = new LineReader(is, job,
					this.recordDelimiterBytes);
		}
	}
}
