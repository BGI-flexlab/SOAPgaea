package org.bgi.flexlab.gaea.tools.mapreduce.fastqqualitycontrol;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FastqQualityControlReducer extends Reducer<Text,Text,NullWritable,Text>{
	FastqQualityControlOptions option;
	
	@Override
	protected void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		option = new FastqQualityControlOptions();
		option.getOptionsFromHadoopConf(conf);
	}
	
	@Override
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		for(Text tx : values){
			context.write(NullWritable.get(), tx);
		}
	}
}
