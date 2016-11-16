package org.bgi.flexlab.gaea.framework.tools.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

public abstract class ToolsRunner extends Configured implements Tool{
	
	protected String toolsDescription = null;
	
	public String getDescription(){
		return toolsDescription;
	}
	
	protected String[] remainArgs(String[] args,Configuration conf){
		try {
			return new GenericOptionsParser(conf, args).getRemainingArgs();
		} catch (IOException e) {
			throw new RuntimeException(e.toString());
		}
	}

	@Override
	abstract public int run(String[] args) throws Exception;
}
