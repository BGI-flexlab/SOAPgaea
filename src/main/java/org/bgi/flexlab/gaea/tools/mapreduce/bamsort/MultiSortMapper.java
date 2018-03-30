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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.bgi.flexlab.gaea.data.mapreduce.input.header.SamHdfsFileHeader;
import org.bgi.flexlab.gaea.data.mapreduce.writable.PairWritable;
import org.bgi.flexlab.gaea.data.mapreduce.writable.SamRecordWritable;
import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;

import java.io.IOException;

public class MultiSortMapper extends
		Mapper<LongWritable, SamRecordWritable, PairWritable, SamRecordWritable> {
	private String type = "all";
	private PairWritable resultKey;
	private SAMFileHeader header;

	@Override
	protected void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		BamSortOptions options = new BamSortOptions();
		options.getOptionsFromHadoopConf(conf);
		type = options.getType();
		SAMFileHeader _header = SamHdfsFileHeader.getHeader(conf);;
		options = new BamSortOptions();
		options.getOptionsFromHadoopConf(conf);
		if(options.getRenames() != null)
			header = BamSortUtils.replaceSampleName(_header.clone(), options.getRenames());
		else
			header = _header;
		resultKey = new PairWritable();
	}

	private boolean filter(GaeaSamRecord sam,Context context){
		boolean result = false;
		if(type.equals("all"))
			result = true;
		else if(type.toLowerCase().equals("unmap")){
			if(sam.getReadUnmappedFlag()){
				context.getCounter("statics", "unmapped read").increment(1);
				result = true;
			}
		}
		return result;
	}

	public void map(LongWritable key, SamRecordWritable value, Context context)
			throws IOException, InterruptedException {
		GaeaSamRecord sam = new GaeaSamRecord(header, value.get());
		String sampleName = sam.getReadGroup().getSample();
		String secondKey = String.format("%03d",sam.getReferenceIndex())
				+ "-" + String.format("%09d",sam.getAlignmentStart())
				+ "-" + String.format("%09d",sam.getAlignmentEnd());
		resultKey.set(sampleName, secondKey);
		if(filter(sam,context)){
			context.write(resultKey, value);
		}
	}
}
