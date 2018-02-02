package org.bgi.flexlab.gaea.tools.mapreduce.haplotypecaller;

import org.apache.hadoop.conf.Configuration;
import org.bgi.flexlab.gaea.data.mapreduce.options.HadoopOptions;
import org.bgi.flexlab.gaea.data.options.GaeaOptions;

public class HaplotypeCallerOptions  extends GaeaOptions implements HadoopOptions{
	
	private String regionFile = null;
	
	private String referenceFile = null;
	
	private int windowsSize = 10000;
	
	private int windowsExtends = 300;

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
	
	public String getReference(){
		return this.referenceFile;
	}
	
	public int getWindowSize(){
		return windowsSize;
	}
	
	public int getWindowsExtendSize(){
		return windowsExtends;
	}
}
