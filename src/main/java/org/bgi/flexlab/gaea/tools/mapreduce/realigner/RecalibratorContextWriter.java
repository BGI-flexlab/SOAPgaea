package org.bgi.flexlab.gaea.tools.mapreduce.realigner;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.bgi.flexlab.gaea.tools.recalibrator.RecalibratorWriter;
import org.bgi.flexlab.gaea.tools.recalibrator.table.RecalibratorTable;

public class RecalibratorContextWriter implements RecalibratorWriter {
	public final static String RECALIBRATOR_TABLE_TAG = "bqsr.table";
	@SuppressWarnings("rawtypes")
	private Context context = null;
	private MultipleOutputs<NullWritable, Text> mos = null;

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public RecalibratorContextWriter(Context ctx,boolean multiple) {
		if(multiple)
			mos = new MultipleOutputs<NullWritable, Text>(ctx);
		else
			this.context = ctx;
	}
	
	public RecalibratorContextWriter(@SuppressWarnings("rawtypes") Context context){
		this(context,false);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void write(RecalibratorTable tables) {
		for (String str : tables.valueStrings()) {
			try {
				if(context != null){
					context.write(NullWritable.get(), new Text(str));
				}else{
					mos.write(RECALIBRATOR_TABLE_TAG, NullWritable.get(), new Text(str));
				}
			} catch (IOException e) {
				throw new RuntimeException(e.toString());
			} catch (InterruptedException e) {
				throw new RuntimeException(e.toString());
			}
		}
	}

	@Override
	public void close() {
		if(mos != null){
			try {
				mos.close();
			} catch (IOException e) {
				throw new RuntimeException(e.toString());
			} catch (InterruptedException e) {
				throw new RuntimeException(e.toString());
			}
		}
	}
}
