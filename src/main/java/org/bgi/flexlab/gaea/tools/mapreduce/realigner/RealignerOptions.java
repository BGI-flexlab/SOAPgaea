package org.bgi.flexlab.gaea.tools.mapreduce.realigner;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.bgi.flexlab.gaea.data.mapreduce.options.HadoopOptions;
import org.bgi.flexlab.gaea.options.GaeaOptions;
import org.seqdoop.hadoop_bam.SAMFormat;

public class RealignerOptions extends GaeaOptions implements HadoopOptions{
	
	private final static String SOFTWARE_NAME = "Realigner";
	private final static String SOFTWARE_VERSION = "1.0";
	
	private int winSize;
	private int reducerNumbers;
	private int maxReadsAtWindows;
	private int extendSize;
	private int snpWindowSize;
	private int minReadsAtPileup;
	private int maxIntervalSize;
	
	private String knowVariant;
	private String input;
	private String output;
	private String reference;
	
	private boolean samFormat;
	private boolean multiSample;
	
	private double mismatchThreshold = 0.0;
	
	public enum AlternateConsensusModel {
		/**
		 * generate alternate consensus model
		 */
		
		/**
		 * uses known indels only.
		 */
		KNOWNS_ONLY,
		/**
		 * uses know indels and the reads.
		 */
		READS,
		/**
		 * uses know indels and the reads and 'Smith-Waterman'.
		 */
		SW
	}
	
	public RealignerOptions(){
		addOption("i", "input", true, "input directory", true);
		addOption("o", "output", true, "output directory", true);
		addOption("r", "reference", true, "reference index(generation by GaeaIndex) file path", true);
		addOption("W", "window", true, "window size for calculating entropy or SNP clusters[10]");
		addOption("w", "keyWindow", true, "window size for key[10000]");
		addOption("e", "windowExtendSize", true, "window extend size[500]");
		addOption("n", "reducer", true, "reducer numbers[30]");
		addOption("k", "knowSite", true, "known snp/indel file,the format is VCF4");
		addOption("M", "multiSample", false, "mutiple sample realignment[false]");
		addOption("s", "samformat", false, "input file is sam format");
		addOption("m", "maxReadsAtWindows", true, "max reads numbers at on windows[1000000]");
		addOption("M", "mismatch", true, "fraction of base qualities needing to mismatch for a position to have high entropy[0]");
		addOption("l", "minReads", true, "minimum reads at a locus to enable using the entropy calculation[4].");
		addOption("L", "intervalLength", true, "max interval length[500].");
		
		FormatHelpInfo(SOFTWARE_NAME, SOFTWARE_VERSION);
	}

	@Override
	public void setHadoopConf(String[] args, Configuration conf) {
		conf.setStrings("args", args);
	}

	@Override
	public void getOptionsFromHadoopConf(Configuration conf) {
		String[] args = conf.getStrings("args");
		this.parse(args);
	}

	@Override
	public void parse(String[] args) {
		try {
			cmdLine = parser.parse(options, args);
		} catch (ParseException e) {
			System.err.println(e.getMessage());
			printHelpInfotmation(SOFTWARE_NAME);
			System.exit(1);
		}
		
		input = getOptionValue("i",null);
		output = getOptionValue("o",null);
		reference = getOptionValue("r",null);
		knowVariant = getOptionValue("k",null);
		
		winSize = getOptionIntValue("w",10000);
		reducerNumbers = getOptionIntValue("n",30);
		maxReadsAtWindows = getOptionIntValue("m",1000000);
		extendSize = getOptionIntValue("e",500);
		snpWindowSize = getOptionIntValue("W",10);
		minReadsAtPileup = getOptionIntValue("l",4);
		maxIntervalSize = getOptionIntValue("L",500);
		
		mismatchThreshold = getOptionDoubleValue("M",0);
		
		samFormat = getOptionBooleanValue("s",false);
		multiSample = getOptionBooleanValue("M",false);
		
		if(output != null && !output.endsWith("/")){
			output += "/";
		}
	}

	public boolean isMultiSample(){
		return multiSample;
	}
	
	public SAMFormat getInputFormat(){
		if(samFormat)
			return SAMFormat.SAM;
		return SAMFormat.BAM;
	}
	
	public int getWindowsSize(){
		return winSize;
	}
	
	public int getReducerNumber(){
		return reducerNumbers;
	}
	
	public int getMaxReadsAtWindows(){
		return this.maxReadsAtWindows;
	}
	
	public String getReference(){
		return reference;
	}
	
	public String getKnowVariant(){
		return knowVariant;
	}
	
	public String getRealignerInput(){
		return input;
	}
	
	public String getRealignerOutput(){
		return output+"realigner";
	}
	
	public String getFixmateInput(){
		return output+"realigner";
	}
	
	public String getFixmateOutput(){
		return output+"fixmate";
	}
	
	public int getExtendSize(){
		return this.extendSize;
	}
	
	public int getSNPWindowSize(){
		return this.snpWindowSize;
	}
	
	public double getMismatchThreshold(){
		return mismatchThreshold;
	}
	
	public int getMinReads(){
		return minReadsAtPileup;
	}
	
	public int getMaxInterval(){
		return maxIntervalSize;
	}
}
