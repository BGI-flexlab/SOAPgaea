package org.bgi.flexlab.gaea.options;

import java.util.HashMap;

public class ToolsOptions extends GaeaOptions {
	private final static String SOFTWARE_VERSION = "1.0";

	public void printHelpInfotmation() {
		helpInfo.setOptPrefix("");
		helpInfo.printHelp("tools list:", options);
	}

	public ToolsOptions(HashMap<String, String> toolsDescription) {
		for (String className : toolsDescription.keySet()) {
			String description = toolsDescription.get(className);
			addOption(className, null, false, description);
		}
		FormatHelpInfo(null, SOFTWARE_VERSION);
	}

	@Override
	public void parse(String[] args) {
	}
}
