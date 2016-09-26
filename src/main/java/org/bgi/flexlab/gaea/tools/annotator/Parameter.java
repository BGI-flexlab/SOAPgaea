/**
 * Copyright (c) 2011, BGI and/or its affiliates. All rights reserved.
 * BGI PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package org.bgi.flexlab.gaea.tools.annotator;

import java.io.Serializable;

import org.apache.commons.cli.ParseException;
import org.bgi.flexlab.gaea.options.GaeaOptions;


public class Parameter extends GaeaOptions implements Serializable {

	private static final long serialVersionUID = -322870818035327827L;
	
	private String configFile = null; //用户配置文件
	private String outputType = null; //输出格式 txt,vcf
	private String outputPath = null; 
	private String inputFilePath = null;
	
	private String referenceSequencePath = null; //参考序列gaeaindex
	
	private boolean mutiSample = false;
	private boolean isCachedRef = false;
	
	private boolean verbose = false;
	private boolean debug = false;

	public Parameter(){}
	
	public Parameter(String[] args) {
		parse(args);
	}
	
	@Override
	public void parse(String[] args) {
		
		addOption("i", "input",      true,  "input file(VCF).", true);
		addOption("o", "output",     true,  "output file(VCF).", true);
		addOption("c", "config",     true,  "config file.", true);
		addOption("r", "reference",  true,  "indexed reference sequence file list [null]", true);
		addOption("T", "outputType", true,  "input file(VCF).");
		addOption(null,"verbose",    false, "display verbose information.");
		addOption(null,"debug",      false, "for debug.");
		addOption("h", "help",       false, "help information.");
		FormatHelpInfo("GaeaAnnotator", "0.0.1-SNAPSHOT");
		
		try {
			cmdLine = parser.parse(options, args);
			if(cmdLine.hasOption("h")) { 
				System.err.println("TEST");
				helpInfo.printHelp("test", options);
				System.exit(0);
			}
		} catch (ParseException e) {
			helpInfo.printHelp("Options:", options, true);
			System.exit(0);
		}
		
		
		inputFilePath = cmdLine.getOptionValue("input");
		outputPath = cmdLine.getOptionValue("output");
		configFile = cmdLine.getOptionValue("config");
		referenceSequencePath = cmdLine.getOptionValue("reference","");
		outputType = getOptionValue("outputType", "txt");
		verbose = getOptionBooleanValue("verbose", false);
		debug = getOptionBooleanValue("debug", false);
	}
	
	public String getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

	public String getOutputType() {
		return outputType;
	}

	public void setOutputType(String outputType) {
		this.outputType = outputType;
	}
	
	public boolean isMutiSample() {
		return mutiSample;
	}

	public void setMutiSample(boolean mutiSample) {
		this.mutiSample = mutiSample;
	}

	public boolean isCachedRef() {
		return isCachedRef;
	}

	public String getInputFilePath() {
		return inputFilePath;
	}
	
	public String setInputFilePath(String inputFilePath) {
		this.inputFilePath = inputFilePath;
		return inputFilePath;
	}
	
	public String getConfigFile() {
		return configFile;
	}
	
	public void setConfigFile(String configFile) {
		this.configFile = configFile;
	}
	
	public String getReferenceSequencePath() {
		return referenceSequencePath;
	}

	public boolean isVerbose() {
		return verbose;
	}

	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}

	public boolean isDebug() {
		return debug;
	}

	public void setDebug(boolean debug) {
		this.debug = debug;
	}
	
	/**
	 * 测试
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		String[] arg = {  "-c", "config.xml" };
		Parameter parameter = new Parameter();
		parameter.parse(arg);
//		System.out.println(parameter.toString());
	}

}
