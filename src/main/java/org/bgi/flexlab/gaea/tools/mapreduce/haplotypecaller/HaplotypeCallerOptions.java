package org.bgi.flexlab.gaea.tools.mapreduce.haplotypecaller;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.bgi.flexlab.gaea.data.mapreduce.options.HadoopOptions;
import org.bgi.flexlab.gaea.data.options.GaeaOptions;

public class HaplotypeCallerOptions  extends GaeaOptions implements HadoopOptions{
	
	private String regionFile = null;
	
	private String referenceFile = null;
	
	private int windowsSize = 10000;
	
	private int windowsExtends = 300;
	
	private List<String> userDisabledReadFilterNames = new ArrayList<>();
	
	private List<String> userEnabledReadFilterNames = new ArrayList<>();

	private  boolean disableToolDefaultReadFilters = false;
	
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
	
	public List<String> getUserDisabledReadFilterNames(){
		return this.userDisabledReadFilterNames;
	}
	
	public List<String> getUserEnabledReadFilterNames(){
		return this.userEnabledReadFilterNames;
	}
	
	public boolean getDisableToolDefaultReadFilters() {
		return this.disableToolDefaultReadFilters;
	}
}
