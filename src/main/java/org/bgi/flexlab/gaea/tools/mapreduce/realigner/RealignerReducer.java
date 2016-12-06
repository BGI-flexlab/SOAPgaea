package org.bgi.flexlab.gaea.tools.mapreduce.realigner;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.bgi.flexlab.gaea.data.mapreduce.input.header.SamHdfsFileHeader;
import org.bgi.flexlab.gaea.data.mapreduce.input.vcf.VCFHdfsLoader;
import org.bgi.flexlab.gaea.data.mapreduce.writable.WindowsBasedWritable;
import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.data.structure.bam.filter.QualityControlFilter;
import org.bgi.flexlab.gaea.data.structure.reference.GenomeShare;
import org.bgi.flexlab.gaea.exception.MissingHeaderException;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.WindowsBasedMapper;
import org.bgi.flexlab.gaea.tools.realigner.RealignerEngine;
import org.bgi.flexlab.gaea.util.SamRecordUtils;
import org.bgi.flexlab.gaea.util.Window;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

public class RealignerReducer
		extends Reducer<WindowsBasedWritable, SAMRecordWritable, NullWritable, SAMRecordWritable> {
	private RealignerOptions option = new RealignerOptions();
	private SAMFileHeader mHeader = null;
	private SAMRecordWritable outputValue = new SAMRecordWritable();
	private QualityControlFilter filter = new QualityControlFilter();

	private ArrayList<GaeaSamRecord> records = new ArrayList<GaeaSamRecord>();
	private ArrayList<GaeaSamRecord> filteredRecords = new ArrayList<GaeaSamRecord>();

	private GenomeShare genomeShare = null;
	private VCFHdfsLoader loader = null;
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

		genomeShare = new GenomeShare();
		genomeShare.loadChromosomeList(option.getReference());

		loader = new VCFHdfsLoader(option.getKnowVariant());
		loader.loadHeader();

		writer = new RealignerContextWriter(context);
		engine = new RealignerEngine(option, genomeShare, loader, mHeader, writer);
	}

	private boolean unmappedWindows(WindowsBasedWritable key) {
		if (key.getChromosomeName().equals(WindowsBasedMapper.UNMAPPED_REFERENCE_NAME))
			return true;
		return false;
	}

	private Window setWindows(WindowsBasedWritable key) {
		int winNum = key.getWindowsNumber();
		int winSize = option.getWindowsSize();
		int start = winNum * winSize;
		int stop = (winNum + 1) * winSize - 1 < mHeader.getSequence(key.getChromosomeName()).getSequenceLength()
				? (winNum + 1) * winSize - 1 : mHeader.getSequence(key.getChromosomeName()).getSequenceLength();

		return new Window(key.getChromosomeName(), start, stop);
	}

	private int getSamRecords(Iterable<SAMRecordWritable> values, ArrayList<GaeaSamRecord> records,
			ArrayList<GaeaSamRecord> filteredRecords, int winNum, Context context) {
		int windowsReadsCounter = 0;
		for (SAMRecordWritable samWritable : values) {
			int readWinNum = samWritable.get().getAlignmentStart() / option.getWindowsSize();
			GaeaSamRecord sam = new GaeaSamRecord(mHeader, samWritable.get(), readWinNum == winNum);
			windowsReadsCounter++;

			if (windowsReadsCounter > option.getMaxReadsAtWindows()) {
				try {
					if (sam.needToOutput()) {
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
	public void reduce(WindowsBasedWritable key, Iterable<SAMRecordWritable> values, Context context)
			throws IOException, InterruptedException {

		boolean unmapped = unmappedWindows(key);

		if (unmapped) {
			for (SAMRecordWritable value : values) {
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
			Window win = setWindows(key);
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
