package org.bgi.flexlab.gaea.data.mapreduce.output.cram;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Paths;

import htsjdk.samtools.CRAMContainerStreamWriter;
import htsjdk.samtools.cram.ref.ReferenceSource;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMTagUtil;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

public class GaeaKeyIgnoringCramRecordWriter<K> extends
		RecordWriter<K, SAMRecordWritable> {
	public final static String OUTPUTFORMAT_REFERENCE = "cram.outputformat.reference";
	private static final String HADOOP_BAM_PART_ID = "Hadoop-BAM-Part";
	private OutputStream origOutput;
	private CRAMContainerStreamWriter cramContainerStream = null;
	private ReferenceSource refSource = null;
	private boolean writeHeader = true;
	private boolean rename = false;
	private String sample = null;
	private Path outputPath = null;

	public GaeaKeyIgnoringCramRecordWriter(Path path, Boolean writerHeader,
			TaskAttemptContext ctx) throws IOException {
		this.outputPath = path;
		init(path, writerHeader, ctx);
	}

	private void init(final Path output, final boolean writeHeader,
			final TaskAttemptContext ctx) throws IOException {
		init(output.getFileSystem(ctx.getConfiguration()).create(output),
				writeHeader, ctx);
	}

	private void init(final OutputStream output, final boolean writeHeader,
			final TaskAttemptContext ctx) throws IOException {
		origOutput = output;
		this.writeHeader = writeHeader;

		final URI referenceURI = URI.create(ctx.getConfiguration().get(
				OUTPUTFORMAT_REFERENCE));

		rename = ctx.getConfiguration().getBoolean("rename.file", false);

		refSource = new ReferenceSource(Paths.get(referenceURI));
	}

	@Override
	public void close(TaskAttemptContext ctx) throws IOException {
		cramContainerStream.finish(false);
		origOutput.close();

		if (rename) {
			final FileSystem srcFS = outputPath.getFileSystem(ctx
					.getConfiguration());
			if (this.sample != null) {
				Path newName = new Path(outputPath.getParent() + "/" + sample
						+ ".sorted.cram");
				srcFS.rename(outputPath, newName);
			}
		}
	}

	protected void writeAlignment(final SAMRecord rec) {
		if (null == cramContainerStream) {
			String rgId = (String) rec
					.getAttribute(SAMTagUtil.getSingleton().RG);
			final SAMFileHeader header = rec.getHeader();
			if (header == null) {
				throw new RuntimeException(
						"Cannot write record to CRAM: null header in SAM record");
			}
			sample = header.getReadGroup(rgId).getSample();
			if (writeHeader) {
				this.writeHeader(header);
			}
			cramContainerStream = new CRAMContainerStreamWriter(origOutput,
					null, refSource, header, HADOOP_BAM_PART_ID);
		}
		cramContainerStream.writeAlignment(rec);
	}

	private void writeHeader(final SAMFileHeader header) {
		cramContainerStream.writeHeader(header);
	}

	@Override
	public void write(K arg0, SAMRecordWritable writable) throws IOException,
			InterruptedException {
		writeAlignment(writable.get());
	}
}
