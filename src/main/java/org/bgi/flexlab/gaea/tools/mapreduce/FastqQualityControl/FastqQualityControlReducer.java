package org.bgi.flexlab.gaea.tools.mapreduce.fastqqualitycontrol;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.bgi.flexlab.gaea.data.structure.reads.report.FastqQualityControlReport;
import org.bgi.flexlab.gaea.tools.fastqqualitycontrol.FastqQualityControlFilter;

public class FastqQualityControlReducer extends Reducer<Text,Text,NullWritable,Text>{
	private FastqQualityControlOptions option;
	private FastqQualityControlFilter filter = null;
	private MultipleOutputs<NullWritable, Text> mos;
	private Text outValue = new Text();
	
	@Override
	protected void setup(Context context) throws IOException {
		mos = new MultipleOutputs<NullWritable, Text>(context);
		Configuration conf = context.getConfiguration();
		option = new FastqQualityControlOptions();
		option.getOptionsFromHadoopConf(conf);
		filter = new FastqQualityControlFilter(option);
	}
	
	@Override
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		ArrayList<String> valueList = new ArrayList<String>();
		for(Text tx : values){
			valueList.add(tx.toString());
		}
		
		String filterResult = filter.filter(valueList);
		if(filter.isDynamicCutted()){
			context.getCounter("Filter counts","dynamic cutted PE reads").increment(1);
		}
		
		if(filterResult != null){
			outValue.set(filterResult);
			context.write(NullWritable.get(), outValue);
		}else{
			context.getCounter("Filter counts","nomal quality control cutted PE reads").increment(1);
		}
		valueList.clear();
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		FastqQualityControlReport report = filter.getReport();
		mos.write("filterStatistic", NullWritable.get(), new Text(report.toString()));
		mos.close();
	}
}
