package org.bgi.flexlab.gaea.options;

import java.util.Properties;

import org.bgi.flexlab.gaea.framework.mapreduce.ToolsRunner;

public class ToolsOptions extends GaeaOptions{
	private final static String SOFTWARE_VERSION = "1.0";
	
	public void printHelpInfotmation() {
		helpInfo.printHelp("tools list:", options);
	}
	
	public ToolsOptions(Properties properties){
		for(Object key : properties.keySet()){
			String className = properties.getProperty((String)key);
			try {
				ToolsRunner tools = (ToolsRunner)(Class.forName(className).newInstance());
				addOption((String)key, null, false, tools.getDescription());
			} catch (InstantiationException | IllegalAccessException
					| ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
		FormatHelpInfo(null,SOFTWARE_VERSION);
	}

	@Override
	public void parse(String[] args) {	
	}
}
