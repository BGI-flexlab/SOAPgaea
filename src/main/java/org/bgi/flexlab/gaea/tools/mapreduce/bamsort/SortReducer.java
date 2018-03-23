package org.bgi.flexlab.gaea.tools.mapreduce.bamsort;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMReadGroupRecord;
import htsjdk.samtools.SAMRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

public final class SortReducer
		extends
		Reducer<LongWritable, SAMRecordWritable, NullWritable, SAMRecordWritable> {
	private SortMultiOutputs<NullWritable, SAMRecordWritable> mos;
	private SAMFileHeader header;
	private Map<String, String> formatSampleName = new HashMap<String, String>();
	private boolean _multiSample = true;
	private String firstSample = null;

	@Override
	protected void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		mos = new SortMultiOutputs<NullWritable, SAMRecordWritable>(context);
		SAMFileHeader _header = BamSortUtils.getHeaderMerger(conf).getMergedHeader();
		String replaceSampleName = conf.get("replace_sample_name",null);
		if(replaceSampleName != null)
			replaceSampleNames(_header.clone(),replaceSampleName);
		else
			header = _header;
		header.setSortOrder(SAMFileHeader.SortOrder.coordinate);
		for (SAMReadGroupRecord rg : header.getReadGroups()) {
			if (!formatSampleName.containsKey(rg.getSample()))
				formatSampleName.put(rg.getSample(),
						BamSortUtils.formatSampleName(rg.getSample()));
			if(firstSample == null)
				firstSample = BamSortUtils.formatSampleName(rg.getSample());
		}
		_multiSample = conf.getBoolean("multi.samples", true);
	}

	public void replaceSampleNames(SAMFileHeader _header,String list) throws IOException{
		FileReader fr = new FileReader(list);
		BufferedReader br = new BufferedReader(fr);

		String line;
		HashMap<String,String> replaceList = new HashMap<String,String>();
		while ((line = br.readLine()) != null) {
			String[] sampleNames = line.split("\t");
			replaceList.put(sampleNames[0], sampleNames[1]);
		}

		br.close();
		fr.close();
		header = BamSortUtils.replaceSampleName(_header.clone(), replaceList);
	}

	@Override
	protected void reduce(
			LongWritable ignored,
			Iterable<SAMRecordWritable> records,
			Context ctx)
			throws IOException, InterruptedException {

		for (SAMRecordWritable rec : records) {
			SAMRecord sam = rec.get();
			sam.setHeader(header);
			String sampleName = sam.getReadGroup().getSample();
			String fileName = formatSampleName.get(sampleName);
			if(!_multiSample)
				fileName = firstSample;
			mos.write(fileName, NullWritable.get(), rec);

		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
	}
}
