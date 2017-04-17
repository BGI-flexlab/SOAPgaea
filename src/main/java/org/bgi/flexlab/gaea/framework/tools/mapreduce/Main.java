/*******************************************************************************
 * Copyright (c) 2017, BGI-Shenzhen
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 *******************************************************************************/
package org.bgi.flexlab.gaea.framework.tools.mapreduce;

import org.bgi.flexlab.gaea.data.options.ToolsOptions;
import org.bgi.flexlab.gaea.util.ArrayUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;

public class Main {
	private static HashMap<String, String> getTools(Properties properties) {
		HashMap<String, String> toolsDescription = new HashMap<String, String>();
		for (Object key : properties.keySet()) {
			String className = properties.getProperty((String) key);
			try {
				ToolsRunner tools = (ToolsRunner) (Class.forName(className)
						.newInstance());
				toolsDescription.put((String) key, tools.getDescription());
			} catch (InstantiationException | IllegalAccessException
					| ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
		}
		return toolsDescription;
	}

	public static void main(String[] args) throws IOException {
		Properties properties = new Properties();
		properties.load(Main.class.getClassLoader().getResourceAsStream(
				"runner.properties"));

		String toolName = null;
		if (args.length > 0)
			toolName = args[0];
		if (toolName != null && properties.getProperty(toolName) != null) {
			String[] arg = ArrayUtils.subArray(args, 1);
			try {
				ToolsRunner tool = (ToolsRunner) (Class.forName(properties
						.getProperty(toolName)).newInstance());
				try {
					tool.run(arg);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			} catch (InstantiationException | IllegalAccessException
					| ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
		} else {
			ToolsOptions option = new ToolsOptions(getTools(properties));
			option.printHelpInfotmation();
		}
	}
}
