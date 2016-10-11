package org.bgi.flexlab.gaea.framework.tools.mapreduce;

import htsjdk.samtools.SAMRecord;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

public class PairEndAggregatorMapper extends
		Mapper<Writable, Writable, Writable, Writable> {
	protected Writable keyout ;
	protected Text textKey = new Text();
	protected Writable valueout;
	
	protected Writable getKey(Writable keyin,Writable valuein){
		if(keyin instanceof Text){
			textKey.set((Text)keyin);
		}else if(keyin instanceof LongWritable){
			SAMRecord sam = ((SAMRecordWritable)valuein).get();
			textKey.set(sam.getReadName());
		}
		return textKey;
	}
	
	protected Writable getValue(Writable valuein){
		return valuein;
	}

	protected void map(Writable key, Writable value, Context context)
			throws IOException, InterruptedException {
		keyout = getKey(key,value);
		valueout = getValue(value);
		context.write(keyout, valueout);
	}
}
