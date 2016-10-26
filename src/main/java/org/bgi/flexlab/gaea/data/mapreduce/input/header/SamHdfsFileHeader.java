package org.bgi.flexlab.gaea.data.mapreduce.input.header;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.bgi.flexlab.gaea.data.structure.header.SamFileHeader;
import org.bgi.flexlab.gaea.exception.FileNotExistException;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMFileReader;
import htsjdk.samtools.SamFileHeaderMerger;

/* *
 * bam header io for hdfs
 * */
public class SamHdfsFileHeader extends SamFileHeader{
	protected final static String BAM_HEADER_FILE_NAME = "SAMFileHeader";
	protected final static SAMFileHeader.SortOrder SORT_ORDER = SAMFileHeader.SortOrder.coordinate;
	protected static boolean MERGE_SEQUENCE_DICTIONARIES = true;
	protected static SAMFileHeader.SortOrder headerMergerSortOrder;
	protected final static boolean ASSUME_SORTED = false;

	public static class HeaderPathFilter implements PathFilter {
		@Override
		public boolean accept(Path path) {
			if (path.getName().startsWith("_"))
				return false;
			return true;
		}
	}

	@SuppressWarnings("deprecation")
	public static SAMFileHeader getSAMHeader(FileSystem fs, Path file)
			throws IOException {
		SAMFileReader samr = new SAMFileReader(fs.open(file));
		SAMFileHeader header = samr.getFileHeader();
		samr.close();
		if (header == null) {
			throw new FileNotExistException.MissingHeaderException(
					file.getName());
		}

		return header;
	}

	/*
	 * merge bam header for input directory
	 * */
	public static SAMFileHeader traversal(Path input, FileSystem fs,
			Configuration conf) {
		ArrayList<SAMFileHeader> mergeHeaders = new ArrayList<SAMFileHeader>();
		SAMFileHeader mergedHeader = null;
		boolean matchedSortOrders = true;
		try {
			if (!fs.exists(input)) {
				throw new FileNotExistException(input.getName());
			}
			if (fs.isFile(input)) {
				SAMFileHeader header = getSAMHeader(fs, input);
				matchedSortOrders = matchedSortOrders
						&& header.getSortOrder() == SORT_ORDER;
				if (!contains(header, mergeHeaders))
					mergeHeaders.add(header);
			} else {
				FileStatus stats[] = fs.listStatus(input,
						new HeaderPathFilter());

				for (FileStatus file : stats) {
					Path filePath = file.getPath();
					SAMFileHeader header = null;
					if (fs.isFile(filePath)) {
						header = getSAMHeader(fs, filePath);
					} else {
						header = traversal(filePath, fs, conf);
					}
					matchedSortOrders = matchedSortOrders
							&& header.getSortOrder() == SORT_ORDER;
					if (!contains(header, mergeHeaders))
						mergeHeaders.add(header);
				}
			}
			if (matchedSortOrders
					|| SORT_ORDER == SAMFileHeader.SortOrder.unsorted
					|| ASSUME_SORTED) {
				headerMergerSortOrder = SORT_ORDER;
			} else {
				headerMergerSortOrder = SAMFileHeader.SortOrder.unsorted;
			}
			mergedHeader = new SamFileHeaderMerger(headerMergerSortOrder,
					mergeHeaders, MERGE_SEQUENCE_DICTIONARIES)
					.getMergedHeader();
		} catch (IOException ioe) {
			throw new RuntimeException(ioe.getMessage());
		}
		return mergedHeader;
	}

	public static SAMFileHeader loadHeader(Path input, Configuration conf,
			Path output) throws IOException {
		FileSystem fs = input.getFileSystem(conf);
		SAMFileHeader mergeHeader = traversal(input, fs, conf);
		if (mergeHeader == null) {
			throw new FileNotExistException.MissingHeaderException(
					input.getName());
		}
		writeHeader(conf, mergeHeader, output);

		return mergeHeader;
	}

	protected static void writeHeader(Configuration conf, SAMFileHeader header,
			Path output) {
		Path rankSumTestObjPath = null;
		FsAction[] v = FsAction.values();
		StringBuilder uri = new StringBuilder();
		uri.append(output);
		if (!output.getName().endsWith("/")) {
			uri.append(System.getProperty("file.separator"));
		}
		uri.append(BAM_HEADER_FILE_NAME);
		conf.set(BAM_HEADER_FILE_NAME, uri.toString());
		rankSumTestObjPath = new Path(uri.toString());
		FileSystem fs = null;
		try {
			fs = rankSumTestObjPath.getFileSystem(conf);
			FsPermission permission = new FsPermission(v[7], v[7], v[7]);
			if (!fs.exists(output)) {
				fs.mkdirs(output, permission);
			} else {
				fs.setPermission(output, permission);
			}
			
			SamFileHeaderCodec.writeHeader(header,fs.create(rankSumTestObjPath));
		} catch (IOException e) {
			throw new RuntimeException(e.toString());
		} finally {
			try {
				fs.close();
			} catch (IOException ioe) {
				throw new RuntimeException(ioe.toString());
			}
		}
	}

	public static SAMFileHeader getHeader(Configuration conf) {
		if(conf.get(BAM_HEADER_FILE_NAME) == null)
			return null;
		
		SAMFileHeader header = null;
		try {
			Path headerPath = new Path(conf.get(BAM_HEADER_FILE_NAME));
			HdfsHeaderLineReader reader = new HdfsHeaderLineReader(headerPath,conf);
			header = SamFileHeaderCodec.readHeader(reader);
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage());
		}

		return header;
	}
}
