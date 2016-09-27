package org.bgi.flexlab.gaea.framework.mapreduce;

import java.io.IOException;
import java.util.Properties;

import org.bgi.flexlab.gaea.options.ToolsOptions;
import org.bgi.flexlab.gaea.util.ArrayUtils;

public class Main {

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
			ToolsOptions option = new ToolsOptions(properties);
			option.printHelpInfotmation();
		}
	}
}
