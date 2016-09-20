package org.bgi.flexlab.gaea.tools.annotator;

import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.Parameter;

/**
 * Parse CommandLine arguments
 *
 */
public class CommandLineParser {

	  @Parameter
	  public List<String> parameters = new ArrayList<>();
	 
//	  @Parameter(names = { "-log", "-verbose" }, description = "Level of verbosity") 
//	  public Integer verbose = 1;
	 
	  @Parameter(names = {"-i","--input"}, description = "Input file(VCF).")
	  public String input;
	  
	  @Parameter(names = {"-o","--output"}, description = "Output file.")
	  public String output;
	  
	  @Parameter(names = {"-ot","--outputType"}, description = "Output file type.")
	  public String outputType;
	  
	  @Parameter(names = {"-c","--config"}, description = "Config file.")
	  public String config;
	 
	  @Parameter(names = "-debug", description = "Debug mode",hidden = true)
	  public boolean debug = false;
	  
	  @Parameter(names = {"-h","--help"}, help = true, description="Print this help message",hidden = true)
	  public boolean help = true;

}