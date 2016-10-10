package org.bgi.flexlab.gaea.framework.tools.mapreduce;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.bgi.flexlab.gaea.data.mapreduce.input.header.SamFileHeader;
import org.bgi.flexlab.gaea.data.mapreduce.writable.CreateDuplicationKey;
import org.bgi.flexlab.gaea.data.mapreduce.writable.DuplicationKeyWritable;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

public class DuplicationMapper extends PairEndAggregatorMapper{
	
	protected SAMFileHeader header = null;
	protected CreateDuplicationKey dupKey = null;
	protected DuplicationKeyWritable dupKeyWritable;
	
	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		header = SamFileHeader.getHeader(conf);
		dupKey = new CreateDuplicationKey(header);
	}
	
	protected Writable getKey(Writable keyin,Writable valuein){
		if(valuein instanceof SAMRecordWritable){
			SAMRecord sam = ((SAMRecordWritable)valuein).get();
			dupKeyWritable = dupKey.getkey(sam);
		}
		return dupKeyWritable;
	}
}
