package org.bgi.flexlab.gaea.tools.mapreduce.haplotypecaller;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.bgi.flexlab.gaea.data.mapreduce.input.bed.RegionHdfsParser;
import org.bgi.flexlab.gaea.data.mapreduce.writable.WindowsBasedWritable;
import org.seqdoop.hadoop_bam.VariantContextWritable;

public class HaplotypeCallerReducer extends Reducer<WindowsBasedWritable, VariantContextWritable, NullWritable, VariantContextWritable>{

	/**
     * region
     */
    private RegionHdfsParser region = null;
    
    private HaplotypeCallerOptions options = new HaplotypeCallerOptions();
    
	@Override
    protected void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		options.getOptionsFromHadoopConf(conf);
		
		if(options.getRegion() != null){
			region = new RegionHdfsParser();
            region.parseBedFileFromHDFS(options.getRegion(), false);
		}
	}
	
	@Override
    public void reduce(WindowsBasedWritable key, Iterable<VariantContextWritable> values, Context context) throws IOException, InterruptedException {
		
	}
	
	@Override
    protected void cleanup(Context context) {

    }
}
