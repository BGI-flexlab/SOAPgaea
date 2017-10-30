package org.bgi.flexlab.gaea.tools.mapreduce.callsv;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.bgi.flexlab.gaea.data.exception.UserException;
import org.bgi.flexlab.gaea.data.mapreduce.options.HadoopOptions;
import org.bgi.flexlab.gaea.data.options.GaeaOptions;

public class CallStructuralVariationOptions extends GaeaOptions implements HadoopOptions{
	private final static String SOFTWARE_NAME = "CallStructuralVariation";
    private final static String SOFTWARE_VERSION = "1.0";
	
	/**
	 * input path of bam/sam files, should be hdfs path
	 */
	private String input;
	/**
	 * hdfs dir for tmp result 
	 */
	private String hdfsdir;
	/**
	 * local results file
	 */
	private String outfile;
	/**
	 * reduce number
	 */
	private int reducenum;
	/**
	 * set std times for range of insert size (mean-stdtimes*std) < normal insertsize < (mean+stdtimes*std)
	 */
	private int stdtimes;
	/**
	 * minimum mapping quality 
	 */
	private int minqual;
	/**
	 * minimum length of a region
	 */
	private int minlen;
	/**
	 * minimum number of read pairs required for a SV
	 */
	private int minpair;
	/**
	 * maximum threshold of haploid sequence coverage for regions to be ignored
	 */
	private int maxcoverage;
	/**
	 * minimum scores for output SVs
	 */
	private int minscore;

	
	public CallStructuralVariationOptions() {
		addOption("i", "input", true, "input path of bam/sam files, should be hdfs path [default: NULL]");
		addOption("hdfs", "hdfsdir", true, "hdfs dir for tmp result [default: NULL]");
		addOption("o", "outfile", true, "local results file [default: ./callSV.ctx]");
		addOption("reducenum", "reducenum", true, "reduce number [default: 1]");
		addOption("s", "stdtimes", true, "set std times for range of insert size (mean-stdtimes*std) < normal insertsize < (mean+stdtimes*std) [default: 3] ");
		addOption("q", "minqual", true, "minimum mapping quality [default: 25]");
		addOption("l", "minlen", true, "minimum length of a region [default: 7]");
		addOption("p", "minpair", true, "minimum number of read pairs required for a SV [default: 2]");
		addOption("c", "maxcoverage", true, "maximum threshold of haploid sequence coverage for regions to be ignored [default: 1000]");
		addOption("score", "minscore", true, "minimum scores for output SVs [default: 30]");
		addOption("h", "help", false, "print help information.");
		
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
            printHelpInfotmation(SOFTWARE_NAME);
            throw new RuntimeException(e);
        }
		
		if(args.length == 0 || getOptionBooleanValue("h", false)) {
            printHelpInfotmation(SOFTWARE_NAME);
            System.exit(1);
        }
		
		input = getOptionValue("i", null);
		hdfsdir = getOptionValue("hdfs", null);
		outfile = getOptionValue("o", "./callSV.ctx");
		reducenum = getOptionIntValue("reducenum", 1);
		stdtimes = getOptionIntValue("s", 3);
		minqual = getOptionIntValue("q", 25);
		minlen = getOptionIntValue("l", 7);
		minpair = getOptionIntValue("p", 2);
		maxcoverage = getOptionIntValue("c", 1000);
		minscore = getOptionIntValue("score", 30);
		
		checkPara();
		
	}
	
	private void checkPara() {
		if(this.input == null)
			throw new UserException.BadArgumentValueException("i", "The input file not found! Please check it");
		
		if(this.hdfsdir == null)
			throw new UserException.BadArgumentValueException("hdfs", "No HDFS dir, please check it");
	
	}


	public String getInput() {
		return input;
	}


	public void setInput(String input) {
		this.input = input;
	}


	public String getHdfsdir() {
		return hdfsdir;
	}


	public void setHdfsdir(String hdfsdir) {
		this.hdfsdir = hdfsdir;
	}


	public String getOutfile() {
		return outfile;
	}


	public void setOutfile(String outfile) {
		this.outfile = outfile;
	}


	public int getReducenum() {
		return reducenum;
	}


	public void setReducenum(int reducenum) {
		this.reducenum = reducenum;
	}


	public int getStdtimes() {
		return stdtimes;
	}


	public void setStdtimes(int stdtimes) {
		this.stdtimes = stdtimes;
	}


	public int getMinqual() {
		return minqual;
	}


	public void setMinqual(int minqual) {
		this.minqual = minqual;
	}


	public int getMinlen() {
		return minlen;
	}


	public void setMinlen(int minlen) {
		this.minlen = minlen;
	}


	public int getMinpair() {
		return minpair;
	}


	public void setMinpair(int minpair) {
		this.minpair = minpair;
	}


	public int getMaxcoverage() {
		return maxcoverage;
	}


	public void setMaxcoverage(int maxcoverage) {
		this.maxcoverage = maxcoverage;
	}


	public int getMinscore() {
		return minscore;
	}


	public void setMinscore(int minscore) {
		this.minscore = minscore;
	}

}
