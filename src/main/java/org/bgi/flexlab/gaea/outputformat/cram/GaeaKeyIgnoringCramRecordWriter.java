package org.bgi.flexlab.gaea.outputformat.cram;

import htsjdk.samtools.CigarElement;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.SAMTagUtil;
import htsjdk.samtools.cram.build.ContainerFactory;
import htsjdk.samtools.cram.build.CramIO;
import htsjdk.samtools.cram.build.FixBAMFileHeader;
import htsjdk.samtools.cram.build.Sam2CramRecordFactory;
import htsjdk.samtools.cram.common.Utils;
import htsjdk.samtools.cram.lossy.QualityScorePreservation;
import htsjdk.samtools.cram.ref.ReferenceSource;
import htsjdk.samtools.cram.ref.ReferenceTracks;
import htsjdk.samtools.cram.structure.Container;
import htsjdk.samtools.cram.structure.CramCompressionRecord;
import htsjdk.samtools.cram.structure.CramHeader;
import htsjdk.samtools.cram.structure.Slice;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bgi.flexlab.gaea.structure.header.UpdateSamHeader;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

public class GaeaKeyIgnoringCramRecordWriter<K> extends
		RecordWriter<K, SAMRecordWritable> {
	public final static String OUTPUTFORMAT_REFERENCE = "cram.outputformat.reference";
	public final static int MAX_CONTAINER_SIZE = 10000;
	
	private Path outputPath = null;
	private OutputStream outputStream = null;
	private boolean isFirstRecord = true;

	private SAMFileHeader samFileHeader = null;
	private int prevSeqId = SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX;
	private String sequenceName = null;
	private SAMSequenceRecord samSequenceRecord = null;
	private ReferenceSource referenceSource = null;
	private FixBAMFileHeader fixBAMFileHeader = null;
	private CramHeader cramHeader = null;
	private long offset;
	private ContainerFactory cf = null;
	private long bases = 0;
	private long coreBytes = 0;
	private long[] externalBytes = new long[10];
	private String sample = null;

	private QualityScorePreservation preservation = null;
	private boolean losslessQS = true;
	private ArrayList<SAMRecord> samRecords = new ArrayList<SAMRecord>(
			MAX_CONTAINER_SIZE);
	private byte[] ref = null;
	private ReferenceTracks tracks = null;
	private boolean captureAllTags = true;
	private boolean preserveReadNames = true;

	private Sam2CramRecordFactory s2cfactory = null;
	private boolean writeHeader = true;
	HashMap<String, String> replaceList = new HashMap<String, String>();

	private boolean rename = false;

	public GaeaKeyIgnoringCramRecordWriter(Path path, Boolean w,
			TaskAttemptContext ctx) throws IOException {
		writeHeader = w;
		this.outputPath = path;
		this.outputStream = this.outputPath.getFileSystem(
				ctx.getConfiguration()).create(this.outputPath);
		initialize(ctx.getConfiguration());
	}

	private void initialize(Configuration conf) {
		captureAllTags = conf.getBoolean("capture-all-tags", true);
		losslessQS = conf.getBoolean("lossless-quality-score", true);
		preserveReadNames = conf.getBoolean("preserveReadNames", true);
		rename = conf.getBoolean("rename.file", false);

		String referenceString = conf.get(OUTPUTFORMAT_REFERENCE);

		File referenceFile = new File(referenceString);
		referenceSource = new ReferenceSource(referenceFile);

		fixBAMFileHeader = new FixBAMFileHeader(referenceSource);
		fixBAMFileHeader.setConfirmMD5(true);
		fixBAMFileHeader.setInjectURI(false);
		fixBAMFileHeader.setIgnoreMD5Mismatch(false);

		if (losslessQS)
			preservation = new QualityScorePreservation("*40");
		else
			preservation = new QualityScorePreservation("");
	}

	@Override
	public void close(TaskAttemptContext ctx) throws IOException,
			InterruptedException {
		if (this.outputStream != null) {
			try {
				writeCurrtenRecord();
				if (writeHeader)
					CramIO.issueZeroB_EOF_marker(outputStream);
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			}
			ref = null;
			this.outputStream.close();

			if (rename) {
				final FileSystem srcFS = outputPath.getFileSystem(ctx
						.getConfiguration());
				if (this.sample != null) {
					Path newName = new Path(outputPath.getParent() + "/"
							+ sample + ".sorted.cram");
					srcFS.rename(outputPath, newName);
				}
			}
		}
	}

	public static void updateTracks(List<SAMRecord> samRecords,
			ReferenceTracks tracks) {
		for (SAMRecord samRecord : samRecords) {
			if (samRecord.getAlignmentStart() != SAMRecord.NO_ALIGNMENT_START) {
				int refPos = samRecord.getAlignmentStart();
				int readPos = 0;
				for (CigarElement ce : samRecord.getCigar().getCigarElements()) {
					if (ce.getOperator().consumesReferenceBases()) {
						for (int i = 0; i < ce.getLength(); i++)
							tracks.addCoverage(refPos + i, 1);
					}
					switch (ce.getOperator()) {
					case M:
					case X:
					case EQ:
						for (int i = readPos; i < ce.getLength(); i++) {
							byte readBase = samRecord.getReadBases()[readPos
									+ i];
							byte refBase = tracks.baseAt(refPos + i);
							if (readBase != refBase)
								tracks.addMismatches(refPos + i, 1);
						}
						break;

					default:
						break;
					}

					readPos += ce.getOperator().consumesReadBases() ? ce
							.getLength() : 0;
					refPos += ce.getOperator().consumesReferenceBases() ? ce
							.getLength() : 0;
				}
			}
		}
	}

	private void initializeHeader(SAMFileHeader header, String sample)
			throws IOException {
		SAMFileHeader newHeader = UpdateSamHeader.createHeaderFromSampleName(
				header, sample);

		samFileHeader = newHeader;
		this.sample = sample;

		cramHeader = new CramHeader(Utils.getMajorVersion(),
				Utils.getMinorVersion(), outputStream == null ? "STDIN"
						: outputPath.getName(), samFileHeader);

		if (writeHeader)
			offset = CramIO.writeCramHeader(cramHeader, outputStream);

		cf = new ContainerFactory(samFileHeader, MAX_CONTAINER_SIZE);
	}

	private void initializeByFirstRecord(SAMRecord sam) {
		sequenceName = sam.getReferenceName();
		prevSeqId = sam.getReferenceIndex();

		if (SAMRecord.NO_ALIGNMENT_REFERENCE_NAME.equals(sequenceName))
			samSequenceRecord = null;
		else
			samSequenceRecord = samFileHeader.getSequence(sequenceName);

		if (samSequenceRecord == null) {
			ref = new byte[0];
			tracks = new ReferenceTracks(
					SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX, ref);
		} else {
			ref = null;
			ref = getReference(samSequenceRecord);
			if (ref == null)
				System.err.println("chrName:"
						+ samSequenceRecord.getSequenceName());
			tracks = new ReferenceTracks(samSequenceRecord.getSequenceIndex(),
					ref);
		}

		initilizeFactory();
		samRecords.add(sam);
	}

	private List<CramCompressionRecord> Sam2CramCovert(
			List<SAMRecord> samRecords) {
		List<CramCompressionRecord> cramRecords = new ArrayList<CramCompressionRecord>(
				samRecords.size());
		int index = 0;
		int prevAlStart = samRecords.get(0).getAlignmentStart();
		updateTracks(samRecords, tracks);

		for (SAMRecord sam : samRecords) {
			CramCompressionRecord cramRecord = s2cfactory.createCramRecord(sam);
			cramRecord.index = ++index;
			cramRecord.alignmentDelta = sam.getAlignmentStart() - prevAlStart;
			cramRecord.alignmentStart = sam.getAlignmentStart();
			prevAlStart = sam.getAlignmentStart();

			cramRecords.add(cramRecord);

			preservation.addQualityScores(sam, cramRecord, tracks);
		}

		Map<String, CramCompressionRecord> mateMap = new TreeMap<String, CramCompressionRecord>();
		for (CramCompressionRecord r : cramRecords) {
			if (!r.isMultiFragment()) {
				r.setDetached(true);

				r.setHasMateDownStream(false);
				r.recordsToNextFragment = -1;
				r.next = null;
				r.previous = null;
			} else {
				String name = r.readName;
				CramCompressionRecord mate = mateMap.get(name);
				if (mate == null) {
					mateMap.put(name, r);
				} else {
					mate.recordsToNextFragment = r.index - mate.index - 1;
					mate.next = r;
					r.previous = mate;
					r.previous.setHasMateDownStream(true);
					r.setHasMateDownStream(false);
					r.setDetached(false);
					r.previous.setDetached(false);

					mateMap.remove(name);
				}
			}
		}

		for (CramCompressionRecord r : mateMap.values()) {
			r.setDetached(true);

			r.setHasMateDownStream(false);
			r.recordsToNextFragment = -1;
			r.next = null;
			r.previous = null;
		}

		return cramRecords;
	}

	private void initilizeFactory() {
		s2cfactory = new Sam2CramRecordFactory(prevSeqId, ref, samFileHeader);
		s2cfactory.captureAllTags = captureAllTags;
		s2cfactory.preserveReadNames = preserveReadNames;
		s2cfactory.ignoreTags.clear();
	}

	public byte[] getReference(SAMSequenceRecord seq) {
		return referenceSource.getReferenceBases(seq, true);
	}

	private void writeCurrtenRecord() {
		if (!samRecords.isEmpty()) {
			List<CramCompressionRecord> records = Sam2CramCovert(samRecords);
			samRecords.clear();

			Container container = null;
			try {
				container = cf.buildContainer(records);
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			for (Slice s : container.slices) {
				s.setRefMD5(ref);
			}
			records.clear();
			long len = 0;
			try {
				len = CramIO.writeContainer(container, outputStream);
			} catch (IOException e) {
				e.printStackTrace();
			}
			container.offset = offset;
			offset += len;

			for (Slice s : container.slices) {
				coreBytes += s.coreBlock.getCompressedContent().length;
				for (Integer i : s.external.keySet())
					externalBytes[i] += s.external.get(i)
							.getCompressedContent().length;
			}
		}
	}

	private void nextRecord(SAMRecord samRecord) {
		if (samRecord.getReferenceIndex() != prevSeqId
				|| samRecords.size() >= MAX_CONTAINER_SIZE) {
			writeCurrtenRecord();
		}

		if (prevSeqId != samRecord.getReferenceIndex()) {
			if (samRecord.getReferenceIndex() != SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX) {
				samSequenceRecord = samFileHeader.getSequence(samRecord
						.getReferenceName());
				ref = null;
				ref = getReference(samSequenceRecord);
				tracks = new ReferenceTracks(
						samSequenceRecord.getSequenceIndex(), ref);

			} else {
				ref = new byte[] {};
				tracks = new ReferenceTracks(
						SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX, ref);
			}
			prevSeqId = samRecord.getReferenceIndex();
			initilizeFactory();
		}

		samRecords.add(samRecord);
		bases += samRecord.getReadLength();
	}

	@Override
	public void write(K ignored, SAMRecordWritable samrecordWritable)
			throws IOException, InterruptedException {
		SAMRecord sam = samrecordWritable.get();
		String rgId = (String) sam.getAttribute(SAMTagUtil.getSingleton().RG);
		String sample = sam.getHeader().getReadGroup(rgId).getSample();
		if (isFirstRecord) {
			try {
				initializeHeader(sam.getHeader(), sample);
			} catch (IOException e) {
				e.printStackTrace();
			}
			initializeByFirstRecord(sam);
			isFirstRecord = false;
			return;
		}
		nextRecord(sam);
	}
}
