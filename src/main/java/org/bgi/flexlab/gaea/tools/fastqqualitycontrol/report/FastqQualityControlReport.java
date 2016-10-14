package org.bgi.flexlab.gaea.tools.fastqqualitycontrol.report;

import org.bgi.flexlab.gaea.data.mapreduce.input.fastq.FastqMultipleSample;
import org.bgi.flexlab.gaea.data.structure.reads.ReadBasicStatic;

public class FastqQualityControlReport {
	private final static int STATIC_COUNT = 24;
	public final static int BASE_STATIC_COUNT = 14;
	private final static int BASE_START_INDEX = BASE_STATIC_COUNT
			- STATIC_COUNT;

	public enum StaticType {
		TOTAL_READS_NUM(0), CLEAN_TOTAL_READS_NUM(1), ADAPTER_READS_NUM(2), TOO_MANY_N_READS_NUM(
				3), TOO_MANY_LOW_QUAL_READS_NUM(4), Adapter_FILTER_DUE_TO_PE(5), tooManyN_FILTER_DUE_TO_PE(
				6), lowQual_FILTER_DUE_TO_PE(7), RAW_TOTAL_BASE_NUM(8), CLEAN_TOTAL_BASE_NUM(
				9), RAW_A_BASE_NUM(10), RAW_C_BASE_NUM(11), RAW_T_BASE_NUM(12), RAW_G_BASE_NUM(
				13), RAW_N_BASE_NUM(14), RAW_Q20_NUM(15), RAW_Q30_NUM(16), CLEAN_A_BASE_NUM(
				17), CLEAN_C_BASE_NUM(18), CLEAN_T_BASE_NUM(19), CLEAN_G_BASE_NUM(
				20), CLEAN_N_BASE_NUM(21), CLEAN_Q20_NUM(22), CLEAN_Q30_NUM(23);

		public final int index;

		private StaticType(int idx) {
			this.index = idx;
		}

		public int get() {
			return index;
		}

		public static int getBaseIndex(int idx) {
			return idx - 10;
		}
	}

	private long[][] statNumbers;

	private ArrayListLongWrap[][] BASE_BY_POSITON;

	private int sampleSize;
	private boolean isMulti = true;

	public FastqQualityControlReport(int ssize, boolean isMulti) {
		this.isMulti = isMulti;

		sampleSize = ssize;
		if (!isMulti)
			sampleSize = 1;

		statNumbers = new long[sampleSize][STATIC_COUNT];

		BASE_BY_POSITON = new ArrayListLongWrap[sampleSize][BASE_STATIC_COUNT];
		initArrayListLongWrap(BASE_BY_POSITON);
	}

	private void initArrayListLongWrap(ArrayListLongWrap[][] data) {
		for (int i = 0; i < sampleSize; i++) {
			for (int j = 0; j < BASE_STATIC_COUNT; j++) {
				data[i][j] = new ArrayListLongWrap();
			}
		}
	}

	public void countRawReadInfo(ReadBasicStatic stat, int sampleID) {
		if (!isMulti) {
			sampleID = 0;
		}
		statNumbers[sampleID][StaticType.TOTAL_READS_NUM.get()] += stat
				.getReadCount();
		statNumbers[sampleID][StaticType.RAW_TOTAL_BASE_NUM.get()] += stat
				.getBaseNumber();

		for (int i = 0; i < (BASE_STATIC_COUNT / 2); i++) {
			statNumbers[sampleID][i+BASE_START_INDEX] += stat.getBasicBaseConunt(i);
		}

		if (stat.getProblemReadsNum() != 0) {
			statNumbers[sampleID][StaticType.ADAPTER_READS_NUM.get()] += stat
					.getAdaptorCount();
			statNumbers[sampleID][StaticType.TOO_MANY_N_READS_NUM.get()] += stat
					.getTooManyNCounter();
			statNumbers[sampleID][StaticType.TOO_MANY_LOW_QUAL_READS_NUM.get()] += stat
					.getLowQualityCounter();
			if (stat.getReadCount() == 2 && stat.getProblemReadsNum() == 1) {
				if (stat.getAdaptorCount() == 1)
					statNumbers[sampleID][StaticType.Adapter_FILTER_DUE_TO_PE
							.get()] += 1;
				else if (stat.getTooManyNCounter() == 1)
					statNumbers[sampleID][StaticType.tooManyN_FILTER_DUE_TO_PE
							.get()] += 1;
				else if (stat.getLowQualityCounter() == 1)
					statNumbers[sampleID][StaticType.lowQual_FILTER_DUE_TO_PE
							.get()] += 1;
			}
		}
	}
	
	public void countCleanReadInfo(ReadBasicStatic stat, int sampleID){
		if (!isMulti) {
			sampleID = 0;
		}
		statNumbers[sampleID][StaticType.TOTAL_READS_NUM.get()] += stat
				.getReadCount();
		statNumbers[sampleID][StaticType.RAW_TOTAL_BASE_NUM.get()] += stat
				.getBaseNumber();

		for (int i = (BASE_STATIC_COUNT / 2); i < BASE_STATIC_COUNT; i++) {
			statNumbers[sampleID][i+BASE_START_INDEX] += stat.getBasicBaseConunt(i);
		}
	}
	
	public void add(ReadBasicStatic stat,int sampleID,boolean isClean){
		if(!isMulti){
			sampleID = 0;
		}
		
		int i;
		for(i = 0 ; i < (BASE_STATIC_COUNT/2) ; i++){
			BASE_BY_POSITON[sampleID][i].add(stat.getPositionInfo(i));
		}
		
		if(isClean){
			int start = BASE_STATIC_COUNT/2;
			for(i = start ; i < BASE_STATIC_COUNT ; i++){
				BASE_BY_POSITON[sampleID][i].add(stat.getPositionInfo(i-start));
			}
		}
	}
}
