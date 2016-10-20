package org.bgi.flexlab.gaea.data.mapreduce.input.header;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMTextHeaderCodec;
import htsjdk.samtools.util.LineReader;

public class SamFileHeaderCodec {

	/**
	 * read sam header from file
	 * 
	 * @param LineReader for htsjdk
	 * @return SAMFileHeader
	 */
	public static SAMFileHeader readHeader(LineReader reader) {
		SAMTextHeaderCodec codec = new SAMTextHeaderCodec();
		SAMFileHeader mHeader = codec.decode(reader, null);
		return mHeader;
	}
	
	/**
	 * write sam header to file for BufferedWriter
	 * 
	 * @param SAMFileHeader
	 * @param BufferedWriter
	 * @return
	 */
	public static void writeHeader(SAMFileHeader header, BufferedWriter writer)
			throws IOException {
		final Writer sw = new StringWriter();
		new SAMTextHeaderCodec().encode(sw, header);

		writer.write(sw.toString());
		writer.close();
	}

	/**
	 * write sam header to file for OutputStream
	 * 
	 * @param SAMFileHeader
	 * @param OutputStream
	 * @return
	 */
	public static void writeHeader(SAMFileHeader header, OutputStream out)
			throws IOException {
		writeHeader(header, new BufferedWriter(new OutputStreamWriter(out)));
	}
}
