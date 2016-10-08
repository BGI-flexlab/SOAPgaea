package org.bgi.flexlab.gaea.data.mapreduce.input.header;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMTextHeaderCodec;

public class SamFileHeaderText {

	public static SAMFileHeader readHeader(HdfsHeaderLineReader reader) {
		SAMTextHeaderCodec codec = new SAMTextHeaderCodec();
		SAMFileHeader mHeader = codec.decode(reader, null);
		
		System.out.println(reader.getLineNumber());
		return mHeader;
	}

	public static void writeHeader(SAMFileHeader header, BufferedWriter writer)
			throws IOException {
		final Writer sw = new StringWriter();
		new SAMTextHeaderCodec().encode(sw, header);

		writer.write(sw.toString());

		writer.close();
	}

	public static void writeHdfsHeader(SAMFileHeader header, Path path,
			Configuration conf) throws IOException {
		FileSystem fs = path.getFileSystem(conf);
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
				fs.create(path)));

		writeHeader(header, writer);
	}
}
