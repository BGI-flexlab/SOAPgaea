package org.bgi.flexlab.gaea.data.structure.reads.report;

import java.math.RoundingMode;
import java.text.DecimalFormat;

import org.bgi.flexlab.gaea.data.structure.reads.ReadBasicStatic;

public class FastqQualityControlReport {
	public final static int STATIC_COUNT = 24;
	public final static int BASE_STATIC_COUNT = 14;
	public final static int BASE_START_INDEX = STATIC_COUNT - BASE_STATIC_COUNT;

	// {
	// TOTAL_READS_NUM(0), CLEAN_TOTAL_READS_NUM(1), ADAPTER_READS_NUM(2),
	// TOO_MANY_N_READS_NUM(
	// 3), TOO_MANY_LOW_QUAL_READS_NUM(4), Adapter_FILTER_DUE_TO_PE(5),
	// tooManyN_FILTER_DUE_TO_PE(
	// 6), lowQual_FILTER_DUE_TO_PE(7), RAW_TOTAL_BASE_NUM(8),
	// CLEAN_TOTAL_BASE_NUM(
	// 9), RAW_A_BASE_NUM(10), RAW_C_BASE_NUM(11), RAW_T_BASE_NUM(12),
	// RAW_G_BASE_NUM(
	// 13), RAW_N_BASE_NUM(14), RAW_Q20_NUM(15), RAW_Q30_NUM(16),
	// CLEAN_A_BASE_NUM(
	// 17), CLEAN_C_BASE_NUM(18), CLEAN_T_BASE_NUM(19), CLEAN_G_BASE_NUM(
	// 20), CLEAN_N_BASE_NUM(21), CLEAN_Q20_NUM(22), CLEAN_Q30_NUM(23);
	// }

	private long[][] statNumbers;

	private ArrayListLongWrap[][] BASE_BY_POSITION;

	private int sampleSize;
	private boolean isMulti = true;

	public FastqQualityControlReport(int ssize, boolean isMulti) {
		this.isMulti = isMulti;

		sampleSize = ssize;
		if (!isMulti)
			sampleSize = 1;

		statNumbers = new long[sampleSize][STATIC_COUNT];

		BASE_BY_POSITION = new ArrayListLongWrap[sampleSize][BASE_STATIC_COUNT];
		initArrayListLongWrap(BASE_BY_POSITION);
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
		statNumbers[sampleID][0] += stat.getReadCount();
		statNumbers[sampleID][8] += stat.getBaseNumber();

		for (int i = 0; i < (BASE_STATIC_COUNT / 2); i++) {
			statNumbers[sampleID][i + BASE_START_INDEX] += stat
					.getBasicBaseCount(i);
		}

		if (stat.getProblemReadsNum() != 0) {
			statNumbers[sampleID][2] += stat.getAdaptorCount();
			statNumbers[sampleID][3] += stat.getTooManyNCounter();
			statNumbers[sampleID][4] += stat.getLowQualityCounter();
			if (stat.getReadCount() == 2 && stat.getProblemReadsNum() == 1) {
				if (stat.getAdaptorCount() == 1)
					statNumbers[sampleID][5] += 1;
				else if (stat.getTooManyNCounter() == 1)
					statNumbers[sampleID][6] += 1;
				else if (stat.getLowQualityCounter() == 1)
					statNumbers[sampleID][7] += 1;
			}
		}
	}

	public void countCleanReadInfo(ReadBasicStatic stat, int sampleID) {
		if (!isMulti) {
			sampleID = 0;
		}
		statNumbers[sampleID][1] += stat.getReadCount();
		statNumbers[sampleID][9] += stat.getBaseNumber();

		for (int i = (BASE_STATIC_COUNT / 2); i < BASE_STATIC_COUNT; i++) {
			statNumbers[sampleID][i + BASE_START_INDEX] += stat
					.getBasicBaseCount(i - (BASE_STATIC_COUNT / 2));
		}
	}

	public void countBaseByPosition(ReadBasicStatic stat, int sampleID,
			boolean isClean) {
		if (!isMulti) {
			sampleID = 0;
		}

		int i;
		for (i = 0; i < (BASE_STATIC_COUNT / 2); i++) {
			BASE_BY_POSITION[sampleID][i].add(stat.getPositionInfo(i));
		}

		if (isClean) {
			int start = BASE_STATIC_COUNT / 2;
			for (i = start; i < BASE_STATIC_COUNT; i++) {
				BASE_BY_POSITION[sampleID][i].add(stat.getPositionInfo(i
						- start));
			}
		}
	}

	public long getCount(int sampleID, int index) {
		if (!isMulti)
			sampleID = 0;
		return statNumbers[sampleID][index];
	}

	public ArrayListLongWrap getBaseByPosition(int sampleID, int index) {
		if (!isMulti)
			sampleID = 0;
		return BASE_BY_POSITION[sampleID][index];
	}

	private boolean partitionNull = false;

	public boolean isPartitionNull() {
		return partitionNull;
	}

	public int addCount(String line) {
		String[] str = line.split("\t");
		int sampleID = Integer.parseInt(str[0]);

		if (!isMulti)
			sampleID = 0;

		for (int i = 1; i < str.length; i++) {
			statNumbers[sampleID][i - 1] += Long.parseLong(str[i]);
		}

		partitionNull = false;
		if (Long.parseLong(str[1]) == 0)
			partitionNull = true;

		return sampleID;
	}

	public void addBaseByPosition(int sampleID, int index, String line) {
		String[] str = line.split("\t");
		for (int i = 0; i < str.length; i++) {
			if (str[i].equals(""))
				continue;
			BASE_BY_POSITION[sampleID][index].add(i, Long.parseLong(str[i]));
		}
	}

	public String toString() {
		StringBuilder strBuilder = new StringBuilder();

		int i, j;
		for (i = 0; i < sampleSize; i++) {
			strBuilder.append(i);

			for (j = 0; j < FastqQualityControlReport.STATIC_COUNT; j++) {
				strBuilder.append("\t" + getCount(i, j));
			}

			if (getCount(i, 0) == 0) {
				continue;
			} else
				strBuilder.append("\n");

			for (j = 0; j < FastqQualityControlReport.BASE_STATIC_COUNT; j++) {
				strBuilder.append(getBaseByPosition(i, j).toString());
				if (j != (FastqQualityControlReport.BASE_STATIC_COUNT - 1))
					strBuilder.append("\n");
			}
		}
		return strBuilder.toString();
	}

	public String getReportContext(int sampleID) {
		DecimalFormat df = new DecimalFormat("0.000");
		df.setRoundingMode(RoundingMode.HALF_UP);

		StringBuilder outString = new StringBuilder();
		outString.append("Filter Information:\n");
		outString.append("Total reads number: ");
		outString.append(statNumbers[sampleID][0]);
		outString.append("\nClean reads number:");
		outString.append(statNumbers[sampleID][1]);
		outString.append("\nAdapter reads number: ");
		outString.append(statNumbers[sampleID][2]);
		outString.append("\nToo many low quality reads number: ");
		outString.append(statNumbers[sampleID][4]);
		outString.append("\nToo many N bases reads number: ");
		outString.append(statNumbers[sampleID][3]);
		outString
				.append("\nFiltered reads due to another PE reads is adapter: ");
		outString.append(statNumbers[sampleID][5]);
		outString
				.append("\nFiltered reads due to another PE reads has too many low quality bases: ");
		outString.append(statNumbers[sampleID][7]);
		outString
				.append("\nFiltered reads due to another PE reads has too many N bases: ");
		outString.append(statNumbers[sampleID][6]);
		outString.append("\nFiltered reads persentage: ");
		outString.append(df.format(100
				* (statNumbers[sampleID][0] - statNumbers[sampleID][1])
				/ (double) statNumbers[sampleID][0]));
		outString.append("%");
		outString.append("\n\nRaw Reads Information:\n");
		outString.append("Total base Number: ");
		outString.append(statNumbers[sampleID][8]);
		outString.append("\nGC rate: ");
		outString
				.append(df
						.format(100
								* (statNumbers[sampleID][11] + statNumbers[sampleID][13])
								/ (double) (statNumbers[sampleID][8] - statNumbers[sampleID][14])));
		outString.append("%\nN base rate: ");
		outString.append(df.format(100 * statNumbers[sampleID][14]
				/ (double) statNumbers[sampleID][8]));
		outString.append("%\nQuality>=20 base rate: ");
		outString.append(df.format(100 * statNumbers[sampleID][15]
				/ (double) statNumbers[sampleID][8]));
		outString.append("%\nQuality>=30 base rate: ");
		outString.append(df.format(100 * statNumbers[sampleID][16]
				/ (double) statNumbers[sampleID][8]));
		outString.append("%\n\nClean Reads Information:\n");
		outString.append("Total base Number: ");
		outString.append(statNumbers[sampleID][9]);
		outString.append("\nGC rate: ");
		outString
				.append(df
						.format(100
								* (statNumbers[sampleID][18] + statNumbers[sampleID][20])
								/ (double) (statNumbers[sampleID][9] - statNumbers[sampleID][21])));
		outString.append("%\nN base rate: ");
		outString.append(df.format(100 * statNumbers[sampleID][21]
				/ (double) statNumbers[sampleID][9]));
		outString.append("%\nQuality>=20 base rate: ");
		outString.append(df.format(100 * statNumbers[sampleID][22]
				/ (double) statNumbers[sampleID][9]));
		outString.append("%\nQuality>=30 base rate: ");
		outString.append(df.format(100 * statNumbers[sampleID][23]
				/ (double) statNumbers[sampleID][9]));
		outString.append("%\n\n\n");
		return outString.toString();
	}

	public String getGraphContext(int sampleID) {
		StringBuilder sb = new StringBuilder();
		int i;
		for (i = 0; i < 5; i++) {
			sb.append(BASE_BY_POSITION[sampleID][i].toString());
		}
		for (i = 7; i < 12; i++) {
			sb.append(BASE_BY_POSITION[sampleID][i].toString());
		}
		for (i = 5; i < 7; i++) {
			sb.append(BASE_BY_POSITION[sampleID][i].toString());
		}
		for (i = 12; i < 14; i++) {
			sb.append(BASE_BY_POSITION[sampleID][i].toString());
		}

		return sb.toString();
	}
}
