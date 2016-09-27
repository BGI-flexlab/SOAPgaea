package org.bgi.flexlab.gaea.framework.mapreduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

public abstract class ToolsRunner extends Configured implements Tool{
	
	protected String toolsDescription = null;
	
	public String getDescription(){
		return toolsDescription;
	}

	@Override
	abstract public int run(String[] args) throws Exception;
}
