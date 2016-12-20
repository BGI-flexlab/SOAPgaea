package org.bgi.flexlab.gaea.tools.mapreduce.recalibrator;

import java.util.ArrayList;

import org.bgi.flexlab.gaea.data.mapreduce.writable.SamRecordWritable;
import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.data.structure.bam.filter.BaseRecalibrationFilter;
import org.bgi.flexlab.gaea.data.structure.reference.BaseAndSNPInformation;
import org.bgi.flexlab.gaea.data.structure.reference.ReferenceShare;
import org.bgi.flexlab.gaea.tools.mapreduce.recalibrator.RecalibratorUtil.Consistent;
import org.bgi.flexlab.gaea.tools.mapreduce.recalibrator.RecalibratorUtil.SolidRecallMode;
import org.bgi.flexlab.gaea.tools.mapreduce.recalibrator.covariate.Covariate;
import org.bgi.flexlab.gaea.tools.mapreduce.recalibrator.covariate.CovariateUtil;
import org.bgi.flexlab.gaea.util.AlignmentUtil;
import org.bgi.flexlab.gaea.util.BaseUtils;
import org.bgi.flexlab.gaea.util.NestedObjectArray;

import htsjdk.samtools.SAMFileHeader;

public class RecalibratorEngine {
	private SAMFileHeader mHeader = null;
	private BaseRecalibratorOptions option = new BaseRecalibratorOptions();
	private ReferenceShare chrInfo = null;
	private BaseAndSNPInformation information = null;
	private BaseRecalibrationFilter filter = null;
	private Covariate[] covariates = null;
	private RecalibratorTable recalibratorTables = null;

	public RecalibratorEngine(BaseRecalibratorOptions option, ReferenceShare chrInfo, SAMFileHeader mHeader) {
		this.option = option;
		this.chrInfo = chrInfo;
		this.mHeader = mHeader;
		information = new BaseAndSNPInformation();
		filter = new BaseRecalibrationFilter();
		recalibratorTables = new RecalibratorTable(CovariateUtil.initializeCovariates(option, mHeader),
				mHeader.getReadGroups().size());
	}

	private void setWindows(String chrName, int winNum) {
		int start = winNum > 0 ? (winNum - 1) * option.getWindowsSize() : 0;
		int end = (winNum + 2) * option.getWindowsSize() - 1;
		information.set(chrInfo, chrName, start, end);
	}

	public void mapReads(ArrayList<GaeaSamRecord> records, Iterable<SamRecordWritable> iterator,String chrName,int winNum) {
		setWindows(chrName,winNum);
		
		if (records != null) {
			for (GaeaSamRecord sam : records) {
				baseQualityStatistics(sam);
			}
		}

		if (iterator == null)
			return;

		for (SamRecordWritable writable : iterator) {
			int readWinNum = writable.get().getAlignmentStart() / option.getWindowsSize();
			GaeaSamRecord sam = new GaeaSamRecord(mHeader, writable.get(), readWinNum == winNum);
			baseQualityStatistics(sam);
		}
	}

	private Consistent isColorSpaceConsistent(GaeaSamRecord read) {
		return RecalibratorUtil.isColorSpaceConsistent(option.SOLID_NOCALL_STRATEGY, read);
	}

	private void mapRead(GaeaSamRecord read) {
		if (RecalibratorUtil.isSOLiDRead(read) || option.SOLID_RECAL_MODE == SolidRecallMode.DO_NOTHING)
			return;

		int end = read.getAlignmentEnd();
		int[] readOffsets = AlignmentUtil.readOffsets(read.getCigar(), read.getAlignmentStart(), end);

		byte[] quals = read.getBaseQualities();
		byte[] bases = read.getReadBases();

		Consistent consistent = null;
		ReadCovariates rcovariate = null;
		for (int i = read.getAlignmentStart(); i < end; i++) {
			int qpos = readOffsets[i];
			if (!information.getSNP(i)) {
				if (consistent == null) {
					consistent = isColorSpaceConsistent(read);
					if (consistent.isColorSpaceConsistent())
						return;
					rcovariate = RecalibratorUtil.computeCovariates(read, covariates);
				}
				if (qpos < 0 || quals[qpos] < option.PRESERVE_QSCORES_LESS_THAN
						|| !consistent.isColorSpaceConsistent(qpos, read.getReadNegativeStrandFlag())) {
					continue;
				}
				dataUpdate(qpos, (byte) bases[qpos], quals[qpos], (byte) information.getBase(i), rcovariate);
			}
		}
	}

	public void dataUpdate(final int offset, final byte base, final byte quality, final byte refBase,
			final ReadCovariates readCovariates) {
		final boolean isError = !BaseUtils.basesAreEqual(base, refBase);
		final EventType eventType = EventType.SNP;
		final int[] keys = readCovariates.getKeySet(offset, eventType);
		final int eventIndex = eventType.index;

		final NestedObjectArray<RecalibratorDatum> rgRecalTable = recalibratorTables
				.getTable(RecalibratorTable.Type.READ_GROUP_TABLE);

		final RecalibratorDatum rgPreviousDatum = rgRecalTable.get(keys[0], eventIndex);
		final RecalibratorDatum rgThisDatum = RecalibratorDatum.build(quality, isError);
		if (rgPreviousDatum == null)
			rgRecalTable.put(rgThisDatum, keys[0], eventIndex);
		else
			rgPreviousDatum.combine(rgThisDatum);

		final NestedObjectArray<RecalibratorDatum> qualRecalTable = recalibratorTables
				.getTable(RecalibratorTable.Type.QUALITY_SCORE_TABLE);
		final RecalibratorDatum qualPreviousDatum = qualRecalTable.get(keys[0], keys[1], eventIndex);
		if (qualPreviousDatum == null)
			qualRecalTable.put(RecalibratorDatum.build(quality, isError), keys[0], keys[1], eventIndex);
		else
			qualPreviousDatum.increment(isError);

		for (int i = 2; i < covariates.length; i++) {
			if (keys[i] < 0)
				continue;
			final NestedObjectArray<RecalibratorDatum> covRecalTable = recalibratorTables.getTable(i);
			final RecalibratorDatum covPreviousDatum = covRecalTable.get(keys[0], keys[1], keys[i], eventIndex);
			if (covPreviousDatum == null)
				covRecalTable.put(RecalibratorDatum.build(quality, isError), keys[0], keys[1], keys[i], eventIndex);
			else
				covPreviousDatum.increment(isError);
		}
	}

	private void baseQualityStatistics(GaeaSamRecord read) {
		if (read.needToOutput()) {
			if (!filter.filter(read, null)) {
				mapRead(read);
			}
		}
	}

	public RecalibratorTable getTables() {
		return this.recalibratorTables;
	}
}
