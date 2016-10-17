package org.bgi.flexlab.gaea.tools.fastqqualitycontrol;

import java.io.IOException;
import java.util.ArrayList;

import org.bgi.flexlab.gaea.data.mapreduce.input.fastq.FastqMultipleSample;
import org.bgi.flexlab.gaea.data.structure.reads.ReadBasicStatic;
import org.bgi.flexlab.gaea.data.structure.reads.ReadInformationWithSampleID;
import org.bgi.flexlab.gaea.tools.fastqqualitycontrol.report.FastqQualityControlReport;
import org.bgi.flexlab.gaea.tools.mapreduce.fastqqualitycontrol.FastqQualityControlOptions;

public class FastqQualityControlFilter {
	private FastqQualityControlOptions option;
	private AdaptorDynamicFilter adpFilter;
	private boolean dycut = false;
	private FastqQualityControlReport report;
	private int qualTrimNum = 0;

	public FastqQualityControlFilter(FastqQualityControlOptions option) {
		this.option = option;
		if(option.isQualityTrim())
			qualTrimNum = option.getQuality() - 33;
		adpFilter = new AdaptorDynamicFilter(option);

		int sampleSize = 1;
		if (option.getMultiSampleList() != null
				&& option.getMultiSampleList() != "") {
			FastqMultipleSample multiSampleList = null;
			try {
				multiSampleList = new FastqMultipleSample(
						option.getMultiSampleList(), false);
			} catch (IOException e) {
				e.printStackTrace();
			}
			sampleSize = multiSampleList.getSampleNumber();
		}
		report = new FastqQualityControlReport(sampleSize,option.isMultiStatis());
	}

	public boolean isDynamicCutted() {
		return dycut;
	}

	public ArrayList<ReadInformationWithSampleID> parseFastq(
			ReadBasicStatic stat, ArrayList<String> values) {
		ArrayList<ReadInformationWithSampleID> list = new ArrayList<ReadInformationWithSampleID>();

		for (String value : values) {
			String[] readLine = value.split("\t");
			if (readLine.length == 1) {
				stat.setAdaptor(Integer.parseInt(readLine[0]) - 1);
			} else {
				ReadInformationWithSampleID fqInfo = new ReadInformationWithSampleID(
						readLine);
				int index = readLine[0].lastIndexOf("/");
				String tempkey = readLine[0].substring(index).trim();
				stat.countBase(fqInfo, Integer.parseInt(tempkey) - 1);
				list.add(fqInfo);
			}
		}

		return list;
	}

	public boolean isBadReads(ReadBasicStatic stat,
			ArrayList<ReadInformationWithSampleID> list) {
		if (stat.getReadCount() == 0) {
			return true;
		} else if (stat.getReadCount() == 1) {
			if (!option.isSEdata() && option.isFilterSE())
				return true;
		} else if (stat.getReadCount() > 2) {
			throw new RuntimeException(
					"more than 2 reads under same readsID without 1,2.\n"
							+ list.get(0).getReadName());
		}

		if (stat.isSampleIDException())
			throw new RuntimeException(
					"pair read has difference sample id.\n"
							+ list.get(0).getReadName());

		if (list.size() > 1) {
			if (list.get(0).equals(list.get(1))) {
				throw new RuntimeException(
						"more than 2 reads under same readsID.\n"
								+ list.get(0).getReadName());
			}
		}
		return false;
	}

	public String qualityControlFilter(ArrayList<String> values) {
		ReadBasicStatic stat = new ReadBasicStatic(option);
		StringBuilder consensusSeq = new StringBuilder();
		ArrayList<ReadInformationWithSampleID> list = parseFastq(stat, values);

		if (isBadReads(stat, list))
			return null;
		
		int sampleID = 0;
		if(!stat.getSampleID().equals("+"))
			sampleID = Integer.parseInt(stat.getSampleID());

		for (ReadInformationWithSampleID read : list) {
			report.countRawReadInfo(stat, sampleID);

			if(stat.getProblemReadsNum() == 0){
				report.countCleanReadInfo(stat, sampleID);
			}
			consensusSeq.append(read.toString(qualTrimNum));
		}
		
		boolean isClean = stat.getProblemReadsNum() == 0 ? true : false;
		report.add(stat, sampleID, isClean);

		return consensusSeq.toString();
	}

	public String filter(ArrayList<String> reads) {
		ArrayList<String> dyFilterReads = new ArrayList<String>();
		if (option.isDyncut()) {
			dycut = adpFilter.dyncutFilter(reads, dyFilterReads);
			if (dyFilterReads.size() == 0) {
				return null;
			}
		} else
			dyFilterReads = reads;

		return qualityControlFilter(dyFilterReads);
	}
}
