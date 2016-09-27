package org.bgi.flexlab.gaea.framework.mapreduce.FastqQualityControl;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FastqQualityControlMapper extends Mapper<Text,Text,Text,Text>{
	@Override
	public void map(Text key, Text value,Context context) throws IOException, InterruptedException {
		
	}
}
