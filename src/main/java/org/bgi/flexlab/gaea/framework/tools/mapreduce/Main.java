package org.bgi.flexlab.gaea.framework.tools.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;

import org.bgi.flexlab.gaea.options.ToolsOptions;
import org.bgi.flexlab.gaea.util.ArrayUtils;

public class Main {
	private static HashMap<String,String> getTools(Properties properties){
		HashMap<String,String> toolsDescription = new HashMap<String,String>();
		for(Object key : properties.keySet()){
			String className = properties.getProperty((String)key);
			try {
				ToolsRunner tools = (ToolsRunner)(Class.forName(className).newInstance());
				toolsDescription.put((String)key, tools.getDescription());
			} catch (InstantiationException | IllegalAccessException
					| ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
		return toolsDescription;
	}

	public static void main(String[] args) throws IOException {
		Properties properties = new Properties();
		properties.load(Main.class.getClassLoader().getResourceAsStream(
				"runner.properties"));

		String toolName = args[0];
		if (properties.getProperty(toolName) != null) {
			String[] arg = ArrayUtils.subArray(args, 1);
			try {
				ToolsRunner tool = (ToolsRunner) (Class.forName(properties
						.getProperty(toolName)).newInstance());
				try {
					tool.run(arg);
				} catch (Exception e) {
					e.printStackTrace();
				}
			} catch (InstantiationException | IllegalAccessException
					| ClassNotFoundException e) {
				e.printStackTrace();
			}
		} else {
			ToolsOptions option = new ToolsOptions(getTools(properties));
			option.printHelpInfotmation();
		}
	}
}
