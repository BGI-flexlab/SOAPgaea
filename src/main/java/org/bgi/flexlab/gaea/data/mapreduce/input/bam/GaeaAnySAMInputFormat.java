package org.bgi.flexlab.gaea.data.mapreduce.input.bam;

import hbparquet.hadoop.util.ContextUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.seqdoop.hadoop_bam.FileVirtualSplit;
import org.seqdoop.hadoop_bam.SAMFormat;
import org.seqdoop.hadoop_bam.SAMInputFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

public class GaeaAnySAMInputFormat extends
		FileInputFormat<LongWritable, SAMRecordWritable> {

	public static final String TRUST_EXTS_PROPERTY = "hadoopbam.anysam.trust-exts";

	private final GaeaBamInputFormat bamIF = new GaeaBamInputFormat();
	private final SAMInputFormat samIF = new SAMInputFormat();

	private final Map<Path, SAMFormat> formatMap;

	private Configuration conf;
	private boolean trustExts;

	public GaeaAnySAMInputFormat() {
		this.formatMap = new HashMap<Path, SAMFormat>();
		this.conf = null;
	}

	public GaeaAnySAMInputFormat(Configuration conf) {
		this.formatMap = new HashMap<Path, SAMFormat>();
		this.conf = conf;
		this.trustExts = conf.getBoolean(TRUST_EXTS_PROPERTY, true);
	}

	public SAMFormat getFormat(final Path path) {
		SAMFormat fmt = formatMap.get(path);
		if (fmt != null || formatMap.containsKey(path))
			return fmt;

		if (this.conf == null)
			throw new IllegalStateException("Don't have a Configuration yet");

		if (trustExts) {
			final SAMFormat f = SAMFormat.inferFromFilePath(path);
			if (f != null) {
				formatMap.put(path, f);
				return f;
			}
		}

		try {
			fmt = SAMFormat.inferFromData(path.getFileSystem(conf).open(path));
		} catch (IOException e) {
		}

		formatMap.put(path, fmt);
		return fmt;
	}

	@Override
	public RecordReader<LongWritable, SAMRecordWritable> createRecordReader(
			InputSplit split, TaskAttemptContext ctx)
			throws InterruptedException, IOException {
		final Path path;
		if (split instanceof FileSplit)
			path = ((FileSplit) split).getPath();
		else if (split instanceof FileVirtualSplit)
			path = ((FileVirtualSplit) split).getPath();
		else
			throw new IllegalArgumentException("split '" + split
					+ "' has unknown type: cannot extract path");

		if (this.conf == null)
			this.conf = ContextUtil.getConfiguration(ctx);

		final SAMFormat fmt = getFormat(path);
		if (fmt == null)
			throw new IllegalArgumentException(
					"unknown SAM format, cannot create RecordReader: " + path);

		switch (fmt) {
		case SAM:
			return samIF.createRecordReader(split, ctx);
		case BAM:
			return bamIF.createRecordReader(split, ctx);
		default:
			assert false;
			return null;
		}
	}

	@Override
	public boolean isSplitable(JobContext job, Path path) {
		if (this.conf == null)
			this.conf = ContextUtil.getConfiguration(job);

		final SAMFormat fmt = getFormat(path);
		if (fmt == null)
			return super.isSplitable(job, path);

		switch (fmt) {
		case SAM:
			return samIF.isSplitable(job, path);
		case BAM:
			return bamIF.isSplitable(job, path);
		default:
			assert false;
			return false;
		}
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		if (this.conf == null)
			this.conf = ContextUtil.getConfiguration(job);

		final List<InputSplit> origSplits = super.getSplits(job);

		final List<InputSplit> bamOrigSplits = new ArrayList<InputSplit>(
				origSplits.size()), newSplits = new ArrayList<InputSplit>(
				origSplits.size());

		for (final InputSplit iSplit : origSplits) {
			final FileSplit split = (FileSplit) iSplit;

			if (SAMFormat.BAM.equals(getFormat(split.getPath())))
				bamOrigSplits.add(split);
			else
				newSplits.add(split);
		}
		newSplits.addAll(bamIF.getSplits(bamOrigSplits,
				ContextUtil.getConfiguration(job)));
		return newSplits;
	}
}
