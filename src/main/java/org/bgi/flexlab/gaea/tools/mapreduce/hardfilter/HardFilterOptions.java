package org.bgi.flexlab.gaea.tools.mapreduce.hardfilter;

import java.io.IOException;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.bgi.flexlab.gaea.data.mapreduce.options.HadoopOptions;
import org.bgi.flexlab.gaea.data.options.GaeaOptions;
import org.bgi.flexlab.gaea.data.structure.header.GaeaVCFHeader;
import org.seqdoop.hadoop_bam.KeyIgnoringVCFOutputFormat;
import org.seqdoop.hadoop_bam.VCFOutputFormat;

public class HardFilterOptions extends GaeaOptions implements HadoopOptions{
	private final static String SOFTWARE_NAME = "HardFilter";
	private final static String SOFTWARE_VERSION = "1.0";
	
	public HardFilterOptions() {
		addOption("i", "input", true, "input VCF or VCFs(separated by comma)", true);
		addOption("o", "output", true, "output path for the resultant VCF file", true);
		addOption("f", "filterName", true, "filter name");
		addOption("s", "snpFilter", true, "hard filters, snp filter parameter, eg:MQ>10");
		addOption("d", "indelFilter", true, "hard filters, indel filter parameter, eg:\"MQ<10||FS>8.0\"\n");
		addOption("h", "help", false, "help information");
	}
	
	private String input;
	
	private String output;
	
	private String filterName;
	
	private String snpFilter;
	
	private String indelFilter;
	
	@Override
	public void setHadoopConf(String[] args, Configuration conf) {
		// TODO Auto-generated method stub
		try {
			String[] otherArgs = new GenericOptionsParser(args).getRemainingArgs();
			conf.setStrings("args", otherArgs);
			conf.set(VCFOutputFormat.OUTPUT_VCF_FORMAT_PROPERTY, "VCF");
			conf.set(GaeaVCFHeader.VCF_HEADER_PROPERTY, setOutputURI("vcfHeader.obj"));
			conf.setBoolean(KeyIgnoringVCFOutputFormat.WRITE_HEADER_PROPERTY, false);
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
	}

	private String setOutputURI(String outputPath){
		StringBuilder uri = new StringBuilder();
		uri.append(output);
		uri.append(System.getProperty("file.separator"));
		uri.append(outputPath);
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
		
		input = getOptionValue("i", null);
		
		output = getOptionValue("o", null);
		
		snpFilter = getOptionValue("s", null);
		
		indelFilter = getOptionValue("d", null);
		
		filterName = getOptionValue("f", "GaeaFilter");
	}

	public String getInput() {
		return input;
	}

	public void setInput(String input) {
		this.input = input;
	}

	public String getOutput() {
		return output;
	}
	
	public void setOutput(String output) {
		this.output = output;
	}
	
	public String getSnpFilter() {
		return snpFilter;
	}
	
	public void setSnpFilter(String snpFilter) {
		this.snpFilter = snpFilter;
	}
	
	public String getIndelFilter() {
		return indelFilter;
	}

	public void setIndelFilter(String indelFilter) {
		this.indelFilter = indelFilter;
	}
	
	public String getFilterName() {
		return filterName;
	}
	
	public String setFilterName() {
		return filterName;
	}
}
