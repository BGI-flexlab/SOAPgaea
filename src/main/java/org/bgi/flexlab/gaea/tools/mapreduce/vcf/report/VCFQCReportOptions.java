package org.bgi.flexlab.gaea.tools.mapreduce.vcf.report;

import java.io.IOException;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.bgi.flexlab.gaea.data.mapreduce.options.HadoopOptions;
import org.bgi.flexlab.gaea.data.options.GaeaOptions;
import org.bgi.flexlab.gaea.data.structure.header.GaeaVCFHeader;

public class VCFQCReportOptions extends GaeaOptions implements HadoopOptions{
	private final static String SOFTWARE_NAME = "VCFQCReport";
	private final static String SOFTWARE_VERSION = "1.0";
	
	public VCFQCReportOptions() {
		// TODO Auto-generated constructor stub
		addOption("i", "input", true, "The vcf file need to be conducted statistics", true);
		addOption("o", "output", true, "The output path for the report file", true);
	}
	
	private String inputs;
	
	private String outputPath;
	
	@Override
	public void setHadoopConf(String[] args, Configuration conf) {
		// TODO Auto-generated method stub
		String[] otherArgs;
		try {
			otherArgs = new GenericOptionsParser(args).getRemainingArgs();
			conf.setStrings("args", otherArgs);
			conf.set(GaeaVCFHeader.VCF_HEADER_PROPERTY, setOutputURI("vcfHeader.obj"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private String setOutputURI(String output){
		StringBuilder uri = new StringBuilder();
		uri.append(outputPath);
		uri.append(System.getProperty("file.separator"));
		uri.append(output);
		return uri.toString();
	}
	
	@Override
	public void getOptionsFromHadoopConf(Configuration conf) {
		// TODO Auto-generated method stub
		String[] args = conf.getStrings("args");
		this.parse(args);
	}

	@Override
	public void parse(String[] args) {
		// TODO Auto-generated method stub
		try {
			cmdLine = parser.parse(options, args);
		} catch (ParseException e) {
			System.err.println(e.getMessage());
			printHelpInfotmation(SOFTWARE_NAME);
			System.exit(1);
		}
		
		inputs = getOptionValue("i", null);
		
		outputPath = getOptionValue("o", null);
	}

	public String getInputs() {
		return inputs;
	}
	
	public void setInputs(String inputs) {
		this.inputs = inputs;
	}
	
	public String getOutputPath() {
		return outputPath;
	}
	
	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}
}
