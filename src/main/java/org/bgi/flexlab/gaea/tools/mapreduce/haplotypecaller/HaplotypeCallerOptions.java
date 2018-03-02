package org.bgi.flexlab.gaea.tools.mapreduce.haplotypecaller;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.bgi.flexlab.gaea.data.mapreduce.options.HadoopOptions;
import org.bgi.flexlab.gaea.data.options.GaeaOptions;
import org.bgi.flexlab.gaea.tools.haplotypecaller.argumentcollection.HaplotypeCallerArgumentCollection;

public class HaplotypeCallerOptions  extends GaeaOptions implements HadoopOptions{
	
	private String regionFile = null;
	
	private String referenceFile = null;
	
	private int windowsSize = 10000;
	
	private int windowsExtends = 300;
	
	private int readShardSize = -1;
	
	private int readPaddingSize = 100;
	
	private List<String> userDisabledReadFilterNames = new ArrayList<>();
	
	private List<String> userEnabledReadFilterNames = new ArrayList<>();

	private  boolean disableToolDefaultReadFilters = false;

	private String dbsnpFile = null;
	
	private String alleleFile = null;
	
	private HaplotypeCallerArgumentCollection hcArgs = new HaplotypeCallerArgumentCollection();
	
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
	
	public int getReadShardSize() {
		return this.readShardSize;
	}
	
	public int getReadShardPadding() {
		return this.readPaddingSize;
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
	
	public String getDBSnp() {
		return dbsnpFile;
	}
	
	public String getAlleleFile() {
		return this.alleleFile;
	}
	
	public HaplotypeCallerArgumentCollection getHaplotypeCallerArguments() {
		return this.hcArgs;
	}
}
