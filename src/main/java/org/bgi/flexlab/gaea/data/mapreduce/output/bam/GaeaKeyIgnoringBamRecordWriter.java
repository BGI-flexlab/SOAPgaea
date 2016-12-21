package org.bgi.flexlab.gaea.data.mapreduce.output.bam;

import htsjdk.samtools.BAMRecordCodec;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.SAMTextHeaderCodec;
import htsjdk.samtools.util.BinaryCodec;
import htsjdk.samtools.util.BlockCompressedOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.io.Writer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bgi.flexlab.gaea.data.mapreduce.writable.SamRecordWritable;

public class GaeaKeyIgnoringBamRecordWriter<K> extends
		RecordWriter<K, SamRecordWritable> {

	private BinaryCodec binaryCodec;
	private BAMRecordCodec bamRecordCodec;
	private Path outputPath;
	private OutputStream outputStream;
	private boolean writeHeader;

	public GaeaKeyIgnoringBamRecordWriter(Path p, Boolean w,
			TaskAttemptContext ctx) throws IOException {
		this.outputPath = p;
		this.outputStream = outputPath.getFileSystem(ctx.getConfiguration()).create(
				outputPath);
		this.writeHeader = w;
	}

	public GaeaKeyIgnoringBamRecordWriter(OutputStream os, Boolean w,
			TaskAttemptContext ctx) {
		this.outputStream = os;
		this.writeHeader = w;
	}

	private void initialize(SAMFileHeader header) {
		OutputStream compressedOut = null;
		if (outputStream != null)
			compressedOut = new BlockCompressedOutputStream(outputStream, null);
		else
			compressedOut = new BlockCompressedOutputStream(outputPath.toString());

		binaryCodec = new BinaryCodec(compressedOut);
		bamRecordCodec = new BAMRecordCodec(header);
		bamRecordCodec.setOutputStream(compressedOut);

		if (writeHeader) {
			writeHeader(header);
		}
	}

	private void writeHeader(final SAMFileHeader header) {
		System.err.println(outputPath.toString() + "\t" + outputStream.toString());
		binaryCodec.writeBytes("BAM\001".getBytes());

		final Writer sw = new StringWriter();
		new SAMTextHeaderCodec().encode(sw, header);

		binaryCodec.writeString(sw.toString(), true, false);

		final SAMSequenceDictionary dict = header.getSequenceDictionary();

		binaryCodec.writeInt(dict.size());
		for (final SAMSequenceRecord rec : dict.getSequences()) {
			binaryCodec.writeString(rec.getSequenceName(), true, true);
			binaryCodec.writeInt(rec.getSequenceLength());
		}
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException,
			InterruptedException {
		if (binaryCodec != null) {
			binaryCodec.close();
		}
	}

	@Override
	public void write(K ignored, SamRecordWritable samRecordWritable) throws IOException,
			InterruptedException {
		SAMRecord sam = samRecordWritable.get();
		if (binaryCodec == null || bamRecordCodec == null) {
			initialize(sam.getHeader());
		}
		
		/*check?*/
		if(sam.getReadUnmappedFlag()){
			sam.setAlignmentStart(0);
		}
		bamRecordCodec.encode(sam);
	}
}
