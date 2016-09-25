/**
 * Copyright (c) 2011, BGI and/or its affiliates. All rights reserved.
 * BGI PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package org.bgi.flexlab.gaea.tools.annotator;

import static java.util.Arrays.asList;

import java.io.IOException;
import java.io.Serializable;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

public class Parameter implements Serializable {

	/**
	 * 序列化版本ID
	 */
	private static final long serialVersionUID = -322870818035327827L;
	
	/**
	 * Reducer个数
	 */
	private int reducerNum=30;

	/**
	 * type
	 */
	private int outputType=0;

	private String inputFilePath=null;
	
	/**
	 * 输出路径
	 */
	private String outputPath=null; 
	
	 /**
	 * 参考序列
	 */
	private String referenceSequencePath=null;
	
	private boolean mutiSample = false;
	private boolean isCachedRef = false;
	
	private String configFile = null;

	/**
	 * 构造函数
	 */
	public Parameter()
	{
		
	}
	
	/**
	 * 构造函数
	 */
	public Parameter(String[] args) {
	
		// 初始化选项解析器
		OptionParser parser = new OptionParser() {
			{
				acceptsAll(asList("i","input"), "Input file(VCF).").withRequiredArg().describedAs("String").ofType(String.class);
				acceptsAll(asList("c","config"), "Config file.").withRequiredArg().describedAs("String").ofType(String.class);
				acceptsAll(asList("r","reference"), "indexed reference sequence file list [null].").withRequiredArg().describedAs("Sting").ofType(String.class);
				acceptsAll(asList("R","reducer"), "The number of reducer,default is 30.").withRequiredArg().describedAs("> 0").ofType(Integer.class);
				acceptsAll(asList("T","outputType"), "input formate[0:txt;1:vcf],default is 0.").withRequiredArg().describedAs(">= 0").ofType(Integer.class);
				acceptsAll(asList("o","output"), "Path of the output files.").withRequiredArg().describedAs("String").ofType(String.class);
				acceptsAll(asList("h","help"), "Display help information.");
			}
		};

		OptionSet options = parser.parse(args);
		
		if (options.has("input")) {
			inputFilePath = (String) options.valueOf("input");
		}

//		if (options.has("reference")) {
//			referenceSequencePath = (String) options.valueOf("reference");
//		}
		
		//inputType
		if(options.has("outputType"))
		{
			setOutputType((Integer) options.valueOf("outputType"));
		}
		if(options.has("output")) {
			outputPath = (String) options.valueOf("output");
		}
		
		if(options.has("mutiSample"))
		{
			mutiSample=true;
		}
		
		if (options.has("h") || inputFilePath == "" || outputPath == "") {
			usage(parser);
			System.exit(0);
		}
		checkPara(parser);
	}


	private void usage(OptionParser parser)
	{
		System.out.println("Software name: GaeaAnnotator");
		System.out.println("Version: 1.00");
		System.out.println("Last update: 2016.9.20");
		System.out.println("Developed by: FlexLab | Science and Technology Division | BGI-shenzhen");
		System.out.println("Copyright(c) 2012: BGI. All Rights Reserved.");
		try {
			parser.printHelpOn(System.out);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void checkPara(OptionParser parser)
	{
		if (inputFilePath==null) {
			System.err.println("ERROR:the input file path is null!");
			usage(parser);
			System.exit(1);
		}
		if (referenceSequencePath==null) {
			System.err.println("ERROR:the referenceSequence path is null");
			usage(parser);
			System.exit(1);
		}
		
		if (outputPath==null) {
			System.err.println("ERROR:the output path is null!");
			usage(parser);
			System.exit(1);
		}
	}
	
	public String getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

	public int getReducerNum() {
		return reducerNum;
	}

	public void setReducerNum(int reducerNum) {
		this.reducerNum = reducerNum;
	}

	public int getOutputType() {
		return outputType;
	}

	public void setOutputType(int outputType) {
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
	
	/**
	 * 主函数
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Parameter parameter = new Parameter(args);
		System.out.println(parameter.toString());
	}


}
