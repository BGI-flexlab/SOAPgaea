package org.bgi.flexlab.gaea.tools.mapreduce.realigner;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.tools.realigner.RealignerWriter;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

public class RealignerContextWriter extends RealignerWriter{

	private Context context = null;
	private SAMRecordWritable value = null;
	
	public RealignerContextWriter(Context context){
		this.context = context;
		this.value = new SAMRecordWritable();
	}
	
	@Override
	public void write(GaeaSamRecord read) {
		value.set(read);
		try {
			context.write(NullWritable.get(), value);
		} catch (IOException | InterruptedException e) {
			throw new RuntimeException(e.toString());
		}
	}

	@Override
	public void close() {
		
	}
}
