package org.bgi.flexlab.gaea.framework.mapreduce.FastqQualityControl;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FastqQualityControlReducer extends Reducer<Text,Text,NullWritable,Text>{
	
	@Override
	protected void setup(Context context) throws IOException {
		
	}
	
	@Override
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		
	}
}
