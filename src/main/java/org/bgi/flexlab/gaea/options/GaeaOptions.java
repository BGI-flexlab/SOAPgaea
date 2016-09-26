package org.bgi.flexlab.gaea.options;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

public abstract class GaeaOptions {
	protected Options options = new Options();
	protected CommandLine cmdLine;
	protected CommandLineParser parser = new PosixParser();
	protected HelpFormatter helpInfo = new HelpFormatter();

	/**
	 * parse parameter
	 */
	abstract public void parse(String[] args);

	/**
	 * help info must use it before parse.
	 */
	public void FormatHelpInfo(String softwareName, String version) {
		StringBuilder sb = new StringBuilder();
		sb.append("Software name: ");
		sb.append(softwareName);
		sb.append("\nVersion: ");
		sb.append(version);
		sb.append("\nLast update: 2015.02.19\n");
		sb.append("Developed by: Bioinformatics core technology laboratory | Science and Technology Division | BGI-shenzhen\n");
		sb.append("Authors: Li ShengKang & Zhang Yong\n");
		sb.append("E-mail: zhangyong2@genomics.org.cn or lishengkang@genomics.cn\n");
		sb.append("Copyright(c) 2015: BGI. All Rights Reserved.\n\n");
		helpInfo.setNewLine("\n");
		helpInfo.setSyntaxPrefix("hadoop jar " + "gaea.jar " + softwareName
				+ " [options]\n" + sb.toString() + "\n");
		helpInfo.setWidth(2 * HelpFormatter.DEFAULT_WIDTH);
	}

	/**
	 * add boolean option
	 */
	protected void addBooleanOption(String opt, String longOpt,
			String description) {
		addOption(opt, longOpt, false, description);
	}

	/**
	 * add normal option.
	 * 
	 * @param opt
	 * @param longOpt
	 * @param hasArg
	 * @param description
	 */
	protected void addOption(String opt, String longOpt, boolean hasArg,
			String description) {
		addOption(opt, longOpt, hasArg, description, false);
	}

	/**
	 * add required option
	 */
	protected void addOption(String opt, String longOpt, boolean hasArg,
			String description, boolean required) {
		Option option = new Option(opt, longOpt, hasArg, description);
		option.setRequired(required);
		options.addOption(option);
	}

	protected String getOptionValue(String opt, String defaultValue) {
		if (cmdLine.hasOption(opt))
			return cmdLine.getOptionValue(opt);

		return defaultValue;
	}
	
	protected int getOptionIntValue(String opt,int defaultValue){
		if(cmdLine.hasOption(opt))
			return Integer.parseInt(cmdLine.getOptionValue(opt));
		return defaultValue;
	}

	protected boolean getOptionBooleanValue(String opt, boolean defaultValue) {
		if (cmdLine.hasOption(opt))
			return true;
		return defaultValue;
	}

	protected double getOptionDoubleValue(String opt, double defaultValue) {
		if (cmdLine.hasOption(opt))
			return Double.parseDouble(cmdLine.getOptionValue(opt));
		return defaultValue;
	}

	protected long getOptionLongValue(String opt,long defaultValue) {
		if(cmdLine.hasOption(opt))
			return Long.parseLong(cmdLine.getOptionValue(opt));
		return defaultValue;
	}
}
