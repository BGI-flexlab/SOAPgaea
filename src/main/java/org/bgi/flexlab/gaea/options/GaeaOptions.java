package org.bgi.flexlab.gaea.options;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

public abstract class GaeaOptions {
	protected Options options = new Options();
	protected CommandLineParser parser = new PosixParser();
	protected CommandLine cmdLine;
	protected HelpFormatter helpInfo = new HelpFormatter();

	/**
	 * set default values of options
	 */
	abstract public void setDefault();

	/**
	 * parse parameter
	 */
	abstract public void parse(String[] args);

	/**
	 * check options
	 */
	abstract public void check();

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
		helpInfo.setSyntaxPrefix("hadoop jar " + softwareName
				+ ".jar [options]\n" + sb.toString() + "\n");
		helpInfo.setWidth(2 * HelpFormatter.DEFAULT_WIDTH);
	}

	/**
	 * add boolean options
	 */
	protected void addBooleanOption(String opt, String longOpt,
			String description) {
		options.addOption(opt, longOpt, false, description);
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
		options.addOption(opt, longOpt, hasArg, description);
	}
	
	protected String getOptionValue(String opt) {
		return getOptionValue(opt);
	}
	
	protected boolean getOptionBooleanValue(String opt){
		return cmdLine.hasOption(opt);
	}

	protected double getOptionDoubleValue(String opt) {
		return Double.parseDouble(cmdLine.getOptionValue(opt));
	}
	
	protected long getOptionLongValue(String opt){
		return Long.parseLong(getOptionValue(opt));
	}
}
