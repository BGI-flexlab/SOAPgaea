package org.bgi.flexlab.gaea.tools.mapreduce.callsv;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bgi.flexlab.gaea.tools.callsv.Format;
import org.bgi.flexlab.gaea.tools.callsv.NewMapKey;

/**
 * 这是第二个MapReducer中的Mapper类 <br>
 * 将第一个MapReducer的输出结果读入，并转换成NewMapKey类型作为key，和Format类型作为value输出给下一个reducer
 * 
 * @author Huifang Lu
 * @version v2.0 <br>
 * 
 */ 
public class CallStructuralVariationMapper2 extends Mapper<LongWritable, Text, NewMapKey, Format>{
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		Format f = new Format(value.toString());
		NewMapKey k = new NewMapKey(f.getChr(), f.getStart());
		context.write(k, f);
		
	}

}
