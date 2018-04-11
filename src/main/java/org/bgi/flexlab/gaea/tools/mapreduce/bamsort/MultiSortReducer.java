/*******************************************************************************
 * Copyright (c) 2017, BGI-Shenzhen
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 *******************************************************************************/
package org.bgi.flexlab.gaea.tools.mapreduce.bamsort;

import htsjdk.samtools.SAMFileHeader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.bgi.flexlab.gaea.data.mapreduce.input.header.SamHdfsFileHeader;
import org.bgi.flexlab.gaea.data.mapreduce.writable.PairWritable;
import org.bgi.flexlab.gaea.data.mapreduce.writable.SamRecordWritable;
import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public final class MultiSortReducer
		extends
		Reducer<PairWritable, SamRecordWritable, NullWritable, SamRecordWritable> {
	private MultipleOutputs<NullWritable,SamRecordWritable> mos;
	private SAMFileHeader header;
//	private Map<String, String> formatSampleName = new HashMap<>();
	private Map<String, SAMFileHeader> sampleHeader = new HashMap<>();
	private BamSortOptions options;

	@Override
	protected void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		SAMFileHeader _header = SamHdfsFileHeader.getHeader(conf);;
		options = new BamSortOptions();
		options.getOptionsFromHadoopConf(conf);
		if(options.getRenames() != null)
			header = BamSortUtils.replaceSampleName(_header.clone(), options.getRenames());
		else
			header = _header;
//		header.setSortOrder(SAMFileHeader.SortOrder.coordinate);
//		for (SAMReadGroupRecord rg : header.getReadGroups()) {
//			if (!formatSampleName.containsKey(rg.getSample()))
//				formatSampleName.put(rg.getSample(),
//						BamSortUtils.formatSampleName(rg.getSample()));
//		}
		mos = new MultipleOutputs<>(context);
	}

	@Override
	protected void reduce(
			PairWritable key,
			Iterable<SamRecordWritable> records,
			Context ctx)
			throws IOException, InterruptedException {

		if(!sampleHeader.containsKey(key.getFirst())) {
			SAMFileHeader newHeader = BamSortUtils.deleteSampleFromHeader(header, key.getFirst());
			sampleHeader.put(key.getFirst(), newHeader);
		}

		for (SamRecordWritable rec : records) {
			GaeaSamRecord sam = new GaeaSamRecord(sampleHeader.get(key.getFirst()), rec.get());
			SamRecordWritable w = new SamRecordWritable();
			w.set(sam);
			mos.write(NullWritable.get(), w, key.getFirst());
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
	}
}
