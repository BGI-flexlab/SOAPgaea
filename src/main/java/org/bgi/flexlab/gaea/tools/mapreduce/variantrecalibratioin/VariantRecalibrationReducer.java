package org.bgi.flexlab.gaea.tools.mapreduce.variantrecalibratioin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

public class VariantRecalibrationReducer extends Reducer<IntWritable, Text, NullWritable, Text>{

}
