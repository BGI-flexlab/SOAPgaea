package org.bgi.flexlab.gaea.data.mapreduce.input.bam;

import htsjdk.samtools.seekablestream.SeekableStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.seqdoop.hadoop_bam.BAMRecordReader;
import org.seqdoop.hadoop_bam.FileVirtualSplit;
import org.seqdoop.hadoop_bam.SAMRecordWritable;
import org.seqdoop.hadoop_bam.SplittingBAMIndex;
import org.seqdoop.hadoop_bam.util.WrapSeekable;

public class GaeaBamInputFormat extends
		FileInputFormat<LongWritable, SAMRecordWritable> {

	private Path getIdxPath(Path path) {
		return path.suffix(".splitting-bai");
	}

	public RecordReader<LongWritable, SAMRecordWritable> createRecordReader(
			InputSplit split, TaskAttemptContext ctx)
			throws InterruptedException, IOException {
		RecordReader<LongWritable, SAMRecordWritable> rr = new BAMRecordReader();
		rr.initialize(split, ctx);
		return rr;
	}

	public List<InputSplit> getSplits(JobContext job) throws IOException {
		return getSplits(super.getSplits(job), job.getConfiguration());
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public List<InputSplit> getSplits(List<InputSplit> splits, Configuration cfg)
			throws IOException {
		Collections.sort(splits, new Comparator() {
			@SuppressWarnings("unused")
			public int compare(InputSplit a, InputSplit b) {
				FileSplit fa = (FileSplit) a;
				FileSplit fb = (FileSplit) b;
				return fa.getPath().compareTo(fb.getPath());
			}

			@Override
			public int compare(Object a, Object b) {
				FileSplit fa = (FileSplit) a;
				FileSplit fb = (FileSplit) b;
				return fa.getPath().compareTo(fb.getPath());
			}
		});
		List<InputSplit> newSplits = new ArrayList<InputSplit>(splits.size());

		for (int i = 0; i < splits.size();) {
			try {
				i = addIndexedSplits(splits, i, newSplits, cfg);
			} catch (IOException e) {
				i = addProbabilisticSplits(splits, i, newSplits, cfg);
			}
		}
		return newSplits;
	}

	private int addIndexedSplits(List<InputSplit> splits, int i,
			List<InputSplit> newSplits, Configuration cfg) throws IOException {
		Path file = ((FileSplit) splits.get(i)).getPath();

		SplittingBAMIndex idx = new SplittingBAMIndex(file.getFileSystem(cfg)
				.open(getIdxPath(file)));

		int splitsEnd = splits.size();
		for (int j = i; j < splitsEnd; j++) {
			if (!file.equals(((FileSplit) splits.get(j)).getPath()))
				splitsEnd = j;
		}
		for (int j = i; j < splitsEnd; j++) {
			FileSplit fileSplit = (FileSplit) splits.get(j);

			long start = fileSplit.getStart();
			long end = start + fileSplit.getLength();

			Long blockStart = idx.nextAlignment(start);

			Long blockEnd = Long.valueOf(j == splitsEnd - 1 ? idx
					.prevAlignment(end).longValue() | 0xFFFF : idx
					.nextAlignment(end).longValue());

			if (blockStart == null) {
				throw new RuntimeException(
						"Internal error or invalid index: no block start for "
								+ start);
			}
			if (blockEnd == null) {
				throw new RuntimeException(
						"Internal error or invalid index: no block end for "
								+ end);
			}
			newSplits.add(new FileVirtualSplit(file, blockStart.longValue(),
					blockEnd.longValue(), fileSplit.getLocations()));
		}
		return splitsEnd;
	}

	private int addProbabilisticSplits(List<InputSplit> splits, int i,
			List<InputSplit> newSplits, Configuration cfg) throws IOException {
		Path path = ((FileSplit) splits.get(i)).getPath();
		SeekableStream sin = WrapSeekable.openPath(path.getFileSystem(cfg),
				path);

		GaeaBamSplitGuesser guesser = new GaeaBamSplitGuesser(sin,cfg);

		FileVirtualSplit previousSplit = null;

		for (; i < splits.size(); i++) {
			FileSplit fspl = (FileSplit) splits.get(i);
			if (!fspl.getPath().equals(path)) {
				break;
			}
			long beg = fspl.getStart();
			long end = beg + fspl.getLength();

			long alignedBeg = guesser.guessNextBAMRecordStart(beg, end);

			long alignedEnd = end << 16 | 0xFFFF;

			if (alignedBeg == end) {
				if (previousSplit == null) {
					System.err
							.println("'"
									+ path
									+ "': "
									+ "no reads in first split: bad BAM file or tiny split size?");
				} else {
					previousSplit.setEndVirtualOffset(alignedEnd);
				}
			} else
				newSplits.add(previousSplit = new FileVirtualSplit(path,
						alignedBeg, alignedEnd, fspl.getLocations()));

		}

		sin.close();
		return i;
	}

	public boolean isSplitable(JobContext job, Path path) {
		return true;
	}

}
