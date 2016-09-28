package org.bgi.flexlab.gaea.framework.tools.mapreduce;

import htsjdk.samtools.SAMRecord;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

public class PairEndAggregatorMapper extends
		Mapper<Writable, Writable, Text, Writable> {
	protected Text keyout = new Text();
	
	protected void set(Writable keyin,Writable valuein){
		if(keyin instanceof Text){
			keyout.set((Text)keyin);
		}else if(keyin instanceof LongWritable){
			SAMRecord sam = ((SAMRecordWritable)valuein).get();
			keyout.set(sam.getReadName());
		}
	}
	
	protected void getValue(Writable valuein,Writable valueout){
	}

	protected void map(Writable key, Writable value, Context context)
			throws IOException, InterruptedException {
		set(key,value);
		getValue(value,value);
		context.write(keyout, value);
	}
}
