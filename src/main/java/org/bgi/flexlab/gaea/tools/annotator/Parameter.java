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
package org.bgi.flexlab.gaea.tools.annotator;

import org.apache.commons.cli.ParseException;
import org.bgi.flexlab.gaea.data.options.GaeaOptions;

import java.io.File;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;


public class Parameter extends GaeaOptions implements Serializable {

	private static final long serialVersionUID = -322870818035327827L;
	
	private String configFile = null; //用户配置文件
//	private String outputType = null; //输出格式 txt,vcf
	private String tmpPath = null; //输出格式 txt,vcf
	private String outputPath = null; 
	private String inputFilePath = null;
	
	private String referenceSequencePath = null; //参考序列gaeaindex
	
	private boolean mutiSample = false;
	private boolean isCachedRef = false;
	
	private boolean verbose = false;
	private boolean debug = false;

	private int mapperNum;

	public Parameter(){}
	
	public Parameter(String[] args) {
		parse(args);
	}
	
	@Override
	public void parse(String[] args) {
		
		addOption("i", "input",      true,  "input file(VCF). [request]", true);
		addOption("o", "output",     true,  "output file of annotation results(.gz) [request]", true);
		addOption("c", "config",     true,  "config file. [request]", true);
		addOption("r", "reference",  true,  "indexed reference sequence file list [request]", true);
		//addOption("T", "outputType", true,  "output file foramt[txt, vcf].");
		addOption("m", "mapperNum", true,  "mapper number. [50]");
		addOption(null,"cacheref",   false,  "DistributedCache reference sequence file list");
		addOption(null,"verbose",    false, "display verbose information.");
		addOption(null,"debug",      false, "for debug.");
		addOption("h", "help",       false, "help information.");
		FormatHelpInfo("GaeaAnnotator", "0.0.1-beta");
		
		try {
			cmdLine = parser.parse(options, args);
			if(cmdLine.hasOption("h")) { 
				helpInfo.printHelp("test", options);
				System.exit(0);
			}
		} catch (ParseException e) {
			helpInfo.printHelp("Options:", options, true);
			System.exit(0);
		}
		
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
	    tmpPath = "/user/" + System.getProperty("user.name") + "/annotmp-" + df.format(new Date());
		
	    setInputFilePath(cmdLine.getOptionValue("input"));
	    setConfigFile(cmdLine.getOptionValue("config"));
	    setReferenceSequencePath(cmdLine.getOptionValue("reference",""));
	    setMapperNum(getOptionIntValue("mapperNum", 50));
	    setOutputPath(cmdLine.getOptionValue("output"));
	    setCachedRef(getOptionBooleanValue("cacheref", false));
		setVerbose(getOptionBooleanValue("verbose", false));
		setDebug(getOptionBooleanValue("debug", false));
	}
	
	private String formatPath(String p) {
		if (p.startsWith("/")) {
			p = "file://" + p;
		}else if (p.startsWith(".")) {
			p = "file://" + new File(p).getAbsolutePath();
		}
		return p;
	}
	
	public String getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

	public String getTmpPath() {
		return tmpPath;
	}

	public void setTmpPath(String tmpPath) {
		this.tmpPath = tmpPath;
	}

	public String getOutputType() {
//		return outputType;
		return "txt";
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

	public void setCachedRef(boolean isCachedRef) {
		this.isCachedRef = isCachedRef;
	}
	
	public String getInputFilePath() {
		return inputFilePath;
	}
	
	public void setInputFilePath(String inputFilePath) {
		this.inputFilePath = formatPath(inputFilePath);
	}
	
	public String getConfigFile() {
		return configFile;
	}
	
	public void setConfigFile(String configFile) {
		this.configFile = formatPath(configFile);
	}
	
	public String getReferenceSequencePath() {
		return referenceSequencePath;
	}
	
	public void setReferenceSequencePath(String referenceSequencePath) {
		this.referenceSequencePath = formatPath(referenceSequencePath);
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

	public int getMapperNum() {
		return mapperNum;
	}

	public void setMapperNum(int mapperNum) {
		this.mapperNum = mapperNum;
	}
}
