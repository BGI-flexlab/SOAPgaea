package org.bgi.flexlab.gaea.tools.mapreduce.realigner;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.bgi.flexlab.gaea.data.exception.MissingHeaderException;
import org.bgi.flexlab.gaea.data.mapreduce.input.header.SamHdfsFileHeader;
import org.bgi.flexlab.gaea.data.mapreduce.writable.SamRecordWritable;
import org.bgi.flexlab.gaea.data.mapreduce.writable.WindowsBasedWritable;
import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.data.structure.bam.filter.QualityControlFilter;
import org.bgi.flexlab.gaea.data.structure.dbsnp.DbsnpShare;
import org.bgi.flexlab.gaea.data.structure.reference.ReferenceShare;
import org.bgi.flexlab.gaea.data.structure.reference.index.VcfIndex;
import org.bgi.flexlab.gaea.data.structure.vcf.VCFLocalLoader;
import org.bgi.flexlab.gaea.tools.realigner.RealignerEngine;
import org.bgi.flexlab.gaea.util.SamRecordUtils;
import org.bgi.flexlab.gaea.util.Window;

public class RealignerReducer
		extends Reducer<WindowsBasedWritable, SamRecordWritable, NullWritable, SamRecordWritable> {
	private RealignerOptions option = new RealignerOptions();
	private SAMFileHeader mHeader = null;
	private SamRecordWritable outputValue = new SamRecordWritable();
	private QualityControlFilter filter = new QualityControlFilter();

	private ArrayList<GaeaSamRecord> records = new ArrayList<GaeaSamRecord>();
	private ArrayList<GaeaSamRecord> filteredRecords = new ArrayList<GaeaSamRecord>();

	private ReferenceShare genomeShare = null;
	private DbsnpShare dbsnpShare = null;
	private VCFLocalLoader loader = null;
	private RealignerEngine engine = null;
	private RealignerContextWriter writer = null;

	@Override
	protected void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		option.getOptionsFromHadoopConf(conf);
		mHeader = SamHdfsFileHeader.getHeader(conf);

		if (mHeader == null) {
			throw new MissingHeaderException("Realigner");
		}

		genomeShare = new ReferenceShare();
		genomeShare.loadChromosomeList(option.getReference());

		dbsnpShare = new DbsnpShare(option.getKnowVariant(), option.getReference());
		dbsnpShare.loadChromosomeList(option.getKnowVariant() + VcfIndex.INDEX_SUFFIX);

		loader = new VCFLocalLoader(option.getKnowVariant());

		writer = new RealignerContextWriter(context);
		engine = new RealignerEngine(option, genomeShare, dbsnpShare, loader, mHeader, writer);
	}

	private boolean unmappedWindows(int chrIndex) {
		if (chrIndex == SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX || chrIndex == -1)
			return true;
		return false;
	}

	private Window setWindows(int chrIndex, int winNum) {
		int winSize = option.getWindowsSize();
		int start = winNum * winSize;

		if (mHeader.getSequence(chrIndex) == null)
			throw new RuntimeException(String.format("chr index %d is not found in reference", chrIndex));
		String chrName = mHeader.getSequence(chrIndex).getSequenceName();
		int stop = (winNum + 1) * winSize - 1 < mHeader.getSequence(chrName).getSequenceLength()
				? (winNum + 1) * winSize - 1 : mHeader.getSequence(chrName).getSequenceLength();

		return new Window(chrName, start, stop);
	}

	private int getSamRecords(Iterable<SamRecordWritable> values, ArrayList<GaeaSamRecord> records,
			ArrayList<GaeaSamRecord> filteredRecords, int winNum, Context context) {
		int windowsReadsCounter = 0;
		for (SamRecordWritable samWritable : values) {
			int readWinNum = samWritable.get().getAlignmentStart() / option.getWindowsSize();
			
			GaeaSamRecord sam = new GaeaSamRecord(mHeader, samWritable.get(), readWinNum == winNum);
			windowsReadsCounter++;

			if (windowsReadsCounter > option.getMaxReadsAtWindows()) {
				try {
					if (sam.needToOutput()) {
						samWritable.get().setHeader(mHeader);
						context.write(NullWritable.get(), samWritable);
					}
				} catch (IOException e) {
					throw new RuntimeException(e.toString());
				} catch (InterruptedException e) {
					throw new RuntimeException(e.toString());
				}
				continue;
			}

			if (SamRecordUtils.isUnmapped(sam)) {
				context.getCounter("ERROR", "unexpect unmapped reads").increment(1);
				try {
					samWritable.get().setHeader(mHeader);
					context.write(NullWritable.get(), samWritable);
				} catch (IOException e) {
					throw new RuntimeException(e.toString());
				} catch (InterruptedException e) {
					throw new RuntimeException(e.toString());
				}
				continue;
			}

			records.add(sam);

			if (!filter.filter(sam, null))
				filteredRecords.add(sam);
		}
		return windowsReadsCounter;
	}

	private void clear() {
		records.clear();
		filteredRecords.clear();
	}

	@Override
	public void reduce(WindowsBasedWritable key, Iterable<SamRecordWritable> values, Context context)
			throws IOException, InterruptedException {
		int chrIndex = key.getChromosomeIndex();
		int winNum = key.getWindowsNumber();
		boolean unmapped = unmappedWindows(chrIndex);

		if (unmapped) {
			for (SamRecordWritable value : values) {
				SAMRecord sam = value.get();
				sam.setHeader(mHeader);
				outputValue.set(sam);
				context.write(NullWritable.get(), outputValue);
			}
			clear();
			return;
		}

		int windowsReadsCounter = getSamRecords(values, records, filteredRecords, key.getWindowsNumber(), context);

		if (windowsReadsCounter > option.getMaxReadsAtWindows()) {
			for (SAMRecord sam : records) {
				outputValue.set(sam);
				context.write(NullWritable.get(), outputValue);
			}
		} else {
			Window win = setWindows(chrIndex, winNum);
			engine.set(win, records, filteredRecords);
			int cnt = engine.reduce();
			context.getCounter("ERROR", "reads input count").increment(cnt);
		}
		clear();
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
	}
}
