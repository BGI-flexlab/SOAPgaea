package org.bgi.flexlab.gaea.tools.mapreduce.vcfstats;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.seqdoop.hadoop_bam.VariantContextWritable;

import java.io.IOException;

public class VCFStatsMapper extends Mapper<LongWritable, VariantContextWritable, Text, VariantContextWritable> {

    protected void setup(Context context)
            throws IOException, InterruptedException {


    }

    @Override
    protected void map(LongWritable key, VariantContextWritable value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
    }

}
