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

import htsjdk.samtools.SAMRecord;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

public class SortMapper extends
		Mapper<LongWritable, SAMRecordWritable, LongWritable, SAMRecordWritable> {
	private String type = "all";
	private HashSet<String> samples = new HashSet<String>();

	@Override
	protected void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		type = conf.get("type");
		if(type.equals("SampleList")){
		}
	}

	private boolean filter(SAMRecord sam,Context context){
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

	public void map(LongWritable key, SAMRecordWritable value, Context context)
			throws IOException, InterruptedException {
		SAMRecord sam = value.get();
		if(filter(sam,context)){
			context.write(key, value);
		}
	}
}
