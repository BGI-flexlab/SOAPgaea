package org.bgi.flexlab.gaea.data.mapreduce.options;

import org.apache.hadoop.conf.Configuration;

public interface HadoopOptions {
	/*set options to hadoop configuration*/
	public void setHadoopConf(String[] args, Configuration conf);
	
	/*get options from hadoop configuration*/
	public void getOptionsFromHadoopConf(Configuration conf);
}
