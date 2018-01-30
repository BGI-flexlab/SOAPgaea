package org.bgi.flexlab.gaea.tools.mapreduce.haplotypecaller;

import org.apache.hadoop.conf.Configuration;
import org.bgi.flexlab.gaea.data.mapreduce.options.HadoopOptions;
import org.bgi.flexlab.gaea.data.options.GaeaOptions;

public class HaplotypeCallerOptions  extends GaeaOptions implements HadoopOptions{
	
	private String regionFile = null;

	@Override
	public void setHadoopConf(String[] args, Configuration conf) {
		
	}

	@Override
	public void getOptionsFromHadoopConf(Configuration conf) {
		
	}

	@Override
	public void parse(String[] args) {
		
	}

	public String getRegion(){
		return this.regionFile;
	}
}
