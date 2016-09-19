package org.bgi.flexlab.gaea.data.mapreduce.input.cram;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.cram.build.ContainerParser;
import htsjdk.samtools.cram.build.Cram2SamRecordFactory;
import htsjdk.samtools.cram.build.CramIO;
import htsjdk.samtools.cram.build.CramNormalizer;
import htsjdk.samtools.cram.build.FixBAMFileHeader;
import htsjdk.samtools.cram.build.FixBAMFileHeader.MD5MismatchError;
import htsjdk.samtools.cram.common.Utils;
import htsjdk.samtools.cram.ref.ReferenceSource;
import htsjdk.samtools.cram.structure.Container;
import htsjdk.samtools.cram.structure.CramCompressionRecord;
import htsjdk.samtools.cram.structure.CramHeader;
import htsjdk.samtools.seekablestream.SeekableStream;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.seqdoop.hadoop_bam.SAMRecordWritable;
import org.seqdoop.hadoop_bam.util.MurmurHash3;
import org.seqdoop.hadoop_bam.util.WrapSeekable;

public class GaeaCramRecordReader extends
		RecordReader<LongWritable, SAMRecordWritable> {

	public final static String INPUTFORMAT_REFERENCE = "inputformat.reference";
	protected final LongWritable key = new LongWritable();
	protected final SAMRecordWritable record = new SAMRecordWritable();
	
	protected ArrayList<SAMRecord> samRecords = new ArrayList<SAMRecord>();
	protected int currentIndex = 0;
	private CramHeader cramHeader = null;
	protected SAMFileHeader samFileHeader = null;
	
	private long currentByte = 0;
	protected long fileStart;
	protected long virtualEnd;
	
	protected SeekableStream sin = null;
	private ReferenceSource referenceSource = null;
	private CramNormalizer cramNormalizer = null;
	private ContainerParser containParser = null;
	private Cram2SamRecordFactory c2sFactory = null;
	protected int prevSeqId = -1;
	private byte[] ref = null;

	/**
	 * Note: this is the only getKey function that handles unmapped reads
	 * specially!
	 */
	public static long getKey(final SAMRecord rec) {
		final int refIdx = rec.getReferenceIndex();
		final int start = rec.getAlignmentStart();

		if (!(rec.getReadUnmappedFlag() || refIdx < 0 || start < 0))
			return getKey(refIdx, start);

		int hash = 0;
		byte[] var;
		if ((var = rec.getVariableBinaryRepresentation()) != null) {
			// Undecoded BAM record: just hash its raw data.
			hash = (int) MurmurHash3.murmurhash3(var, hash);
		} else {
			hash = (int) MurmurHash3.murmurhash3(rec.getReadName(), hash);
			hash = (int) MurmurHash3.murmurhash3(rec.getReadBases(), hash);
			hash = (int) MurmurHash3.murmurhash3(rec.getBaseQualities(), hash);
			hash = (int) MurmurHash3.murmurhash3(rec.getCigarString(), hash);
		}
		hash = Math.abs(hash);
		return getKey0(Integer.MAX_VALUE, hash);
	}

	/**
	 * @param alignmentStart
	 *            1-based leftmost coordinate.
	 */
	public static long getKey(int refIdx, int alignmentStart) {
		return getKey0(refIdx, alignmentStart - 1);
	}

	/**
	 * @param alignmentStart0
	 *            0-based leftmost coordinate.
	 */
	public static long getKey0(int refIdx, int alignmentStart0) {
		return (long) refIdx << 32 | alignmentStart0;
	}

	public void initializeCramHeader(Configuration conf, Path file) {
		if(conf.get(INPUTFORMAT_REFERENCE) == null)
			throw new IllegalArgumentException("refernce cann't be null for cram file!");
		
		File ref = new File(conf.get(INPUTFORMAT_REFERENCE));
		referenceSource = new ReferenceSource(ref);

		FixBAMFileHeader fix = new FixBAMFileHeader(referenceSource);
		fix.setConfirmMD5(true);
		fix.setInjectURI(false);
		fix.setIgnoreMD5Mismatch(false);
		try {
			fix.fixSequences(samFileHeader.getSequenceDictionary()
					.getSequences());
		} catch (MD5MismatchError e) {
			System.exit(1);
		}
		fix.addCramtoolsPG(samFileHeader);
		
		cramNormalizer = new CramNormalizer(samFileHeader, referenceSource);
		containParser = new ContainerParser(samFileHeader);
		c2sFactory = new Cram2SamRecordFactory(samFileHeader);

		cramHeader = new CramHeader(Utils.getMajorVersion(),
				Utils.getMinorVersion(), file == null ? "STDIN" : file.toString(),
				samFileHeader);

		if (cramHeader == null)
			throw new RuntimeException(
					"header is null at cramRecordReader initialize!!!");
	}

	@Override
	public void initialize(InputSplit spl, TaskAttemptContext ctx)
			throws IOException {

		final Configuration conf = ctx.getConfiguration();

		FileSplit split = (FileSplit) spl;
		final Path file = split.getPath();

		final FileSystem fs = file.getFileSystem(conf);

		sin = WrapSeekable.openPath(fs, file);
		fileStart = 0;
		virtualEnd = sin.length();

		if (fileStart == 0) {
			samFileHeader = CramIO.readCramHeader(sin).getSamFileHeader();
		} else{
			sin.seek(fileStart);
		}
		initializeCramHeader(conf, file);
	}

	@Override
	public void close() throws IOException {
		if (sin != null)
			sin.close();
		samRecords.clear();
	}

	/**
	 * Unless the end has been reached, this only takes file position into
	 * account, not the position within the block.
	 */
	@Override
	public float getProgress() {
		if (currentByte >= (virtualEnd - fileStart))
			return 1;
		else {
			return (float) (currentByte) / (virtualEnd - fileStart + 1);
		}
	}

	@Override
	public LongWritable getCurrentKey() {
		return key;
	}

	@Override
	public SAMRecordWritable getCurrentValue() {
		return record;
	}

	public void covertCramRecord2SamRecord(
			ArrayList<CramCompressionRecord> cramRecords) {
		for (CramCompressionRecord r : cramRecords) {
			SAMRecord s = c2sFactory.create(r);
			samRecords.add(s);
			if (ref != null)
				Utils.calculateMdAndNmTags(s, ref, false, false);
		}
	}

	public byte[] getReference(SAMSequenceRecord seq) {
		return referenceSource.getReferenceBases(seq, true);
	}
	
	protected Container getContainer(){
		Container c = null;
		try {
			if (sin == null) {
				throw new RuntimeException("InputStream is null !!!");
			}
			if (sin.position() == sin.length())
				return c;
			c = CramIO.readContainer(cramHeader, sin);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return c;
	}

	public boolean ContainerRead(Container c) {
		if (currentByte >= (virtualEnd - fileStart))
			return false;
		samRecords.clear();

		//Container c = getContainer();
		if(c == null)
			return false;

		currentByte = (c.offset + c.containerByteSize + c.headerSize);
		
		ArrayList<CramCompressionRecord> cramRecords = new ArrayList<CramCompressionRecord>(
				10000);
		try {
			containParser.getRecords(c, cramRecords);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		switch (c.sequenceId) {
		case SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX:
		case -2:
			ref = new byte[] {};
			break;

		default:
			if (prevSeqId < 0 || prevSeqId != c.sequenceId) {
				SAMSequenceRecord sequence = cramHeader.getSamFileHeader()
						.getSequence(c.sequenceId);
				ref = null;
				// ref = referenceSource.getReferenceBases(sequence, true);
				ref = getReference(sequence);
				Utils.upperCase(ref);
				prevSeqId = c.sequenceId;
			}
			break;
		}

		cramNormalizer.normalize(cramRecords, true, ref, c.alignmentStart,
				c.h.substitutionMatrix, c.h.AP_seriesDelta);

		if (cramRecords.size() == 0)
			return false;
		
		covertCramRecord2SamRecord(cramRecords);
		
		currentIndex = 0;
		return true;
	}

	@Override
	public boolean nextKeyValue() {
		/* get new container */
		if ((samRecords.size() > 0 && currentIndex >= samRecords.size())
				|| samRecords.size() == 0) {
			Container c = getContainer();
			if (!ContainerRead(c))
				return false;
		}

		final SAMRecord r = samRecords.get(currentIndex);
		r.setHeader(samFileHeader);
		++currentIndex;

		key.set(getKey(r));
		record.set(r);
		return true;
	}
}
