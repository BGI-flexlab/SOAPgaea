package org.bgi.flexlab.gaea.tools.mapreduce.bamqualitycontrol;

import java.io.IOException;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.bgi.flexlab.gaea.data.mapreduce.options.HadoopOptions;
import org.bgi.flexlab.gaea.data.options.GaeaOptions;

public class BamQualityControlOptions extends GaeaOptions implements HadoopOptions {
	private final static String SOFTWARE_NAME = "Bam Quality Control";
	private final static String SOFTWARE_VERSION = "1.0";
	
	public BamQualityControlOptions() {
		// TODO Auto-generated constructor stub
		addOption("i", "input", true, "Path of the alignment result file(s)", true);
		addOption("o", "output", true, "Path of the bamqc report file", true);
		addOption("d", "index", true, "Path of the reference index list", true);
		addBooleanOption("b", "basic", "Basic mapping Statitis of each sample without coverage info");
		addBooleanOption("l", "lane", "Get basic information by lane rather than samples");
		addOption("r", "reducer", true, "The reducer nums");
		addOption("R", "region", true, "Region string(chr:start-end)");
		addOption("B", "bedFile", true, "bed format file");
		addBooleanOption("D", "distributeCache", "distribute cache the reference");
		addOption("a", "anRegion", true, "annotation regions");
		addOption("c", "cnvRegion", true, "cnv regions in bed format");
		addOption("n", "minDepth", true, "minimum depth for single region statistics");
		addBooleanOption("C", "cnvDepth", "output the cnv depth file");
		addBooleanOption("x", "gender", "inhibit gender prediction");
		addBooleanOption("m", "unmapped", "inhibit unmapped region output");
		addOption("h", "help", false, "help information");
	}

	private String alignmentFilePath;

	/**
	 * 一致性序列文件名称
	 */
	private String outputPath;

	/**
	 * 参考基因组序列文件路径
	 */
	private String referenceSequencePath;

	/**
	 * Reducer个数
	 */
	private int reducerNum;
	
	/**
	 * region in string
	 */
	private String region;
	
	/**
	 * regions in bed file
	 */
	private String bedfile;
	
	/**
	 * basic?
	 */
	private boolean isBasic;
	
	/**
	 * lane?sample?
	 */
	private boolean islane;
	
	/**
	 * distribute cache
	 */
	private boolean distributeCache;
	
	/**
	 * single region
	 */
	private String singleRegion;
	
	/**
	 * minimum depth for singleRegion
	 */
	private int minSingleRegionDepth;
	
	/**
	 * do cnv depth ? 
	 */
	private boolean cnvDepth;
	
	/**
	 * gender predict;
	 */
	private boolean genderPredict;
	
	private boolean outputUnmapped;
	
	private String cnvRegion;

	@Override
	public void setHadoopConf(String[] args, Configuration conf) {
		// TODO Auto-generated method stub
		String[] otherArgs;
		try {
			otherArgs = new GenericOptionsParser(args).getRemainingArgs();
			conf.setStrings("args", otherArgs);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
		
		cnvDepth = getOptionBooleanValue("C", false);
		cnvRegion = getOptionValue("c", null);
		outputUnmapped = getOptionBooleanValue("m", true);
		genderPredict = getOptionBooleanValue("x", true);
		singleRegion = getOptionValue("a", null);
		minSingleRegionDepth = getOptionIntValue("n", 0);
		distributeCache = getOptionBooleanValue("D", false);
		isBasic = getOptionBooleanValue("b", false);
		islane = getOptionBooleanValue("l", false);
		alignmentFilePath = getOptionValue("i", null);
		referenceSequencePath = getOptionValue("d", null);
		outputPath = getOptionValue("o", null);
		reducerNum = getOptionIntValue("r", 30);
		region = getOptionValue("R", null);
		bedfile = getOptionValue("B", null);
	}
	
	/**
	 * @return the alignmentFilePath
	 */
	public String getAlignmentFilePath() {
		return alignmentFilePath;
	}

	/**
	 * @param alignmentFilePath the alignmentFilePath to set
	 */
	public void setAlignmentFilePath(String alignmentFilePath) {
		this.alignmentFilePath = alignmentFilePath;
	}

	/**
	 * @return the outputPath
	 */
	public String getOutputPath() {
		return outputPath;
	}

	public String getTempPath() {
		return this.outputPath + "/tmp";
	}
	
	/**
	 * @param outputPath the outputPath to set
	 */
	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

	/**
	 * @return the referenceSequencePath
	 */
	public String getReferenceSequencePath() {
		return referenceSequencePath;
	}

	/**
	 * @param referenceSequencePath the referenceSequencePath to set
	 */
	public void setReferenceSequencePath(String referenceSequencePath) {
		this.referenceSequencePath = referenceSequencePath;
	}

	/**
	 * @return the region
	 */
	public String getRegion() {
		return region;
	}

	/**
	 * @param region the region to set
	 */
	public void setRegion(String region) {
		this.region = region;
	}

	/**
	 * @return the bedfile
	 */
	public String getBedfile() {
		return bedfile;
	}

	/**
	 * @param bedfile the bedfile to set
	 */
	public void setBedfile(String bedfile) {
		this.bedfile = bedfile;
	}

	public int getReducerNum() {
		return reducerNum;
	}

	public void setReducerNum(int reducerNum) {
		this.reducerNum = reducerNum;
	}

	/**
	 * @return the isBasic
	 */
	public boolean isBasic() {
		return isBasic;
	}

	/**
	 * @param isBasic the isBasic to set
	 */
	public void setBasic(boolean isBasic) {
		this.isBasic = isBasic;
	}

	/**
	 * @return the islane
	 */
	public boolean isIslane() {
		return islane;
	}

	/**
	 * @param islane the islane to set
	 */
	public void setIslane(boolean islane) {
		this.islane = islane;
	}

	/**
	 * @return the distributeCache
	 */
	public boolean isDistributeCache() {
		return distributeCache;
	}

	/**
	 * @param distributeCache the distributeCache to set
	 */
	public void setDistributeCache(boolean distributeCache) {
		this.distributeCache = distributeCache;
	}

	/**
	 * @return the singleRegion
	 */
	public String getSingleRegion() {
		return singleRegion;
	}

	/**
	 * @param singleRegion the singleRegion to set
	 */
	public void setSingleRegion(String singleRegion) {
		this.singleRegion = singleRegion;
	}

	/**
	 * @return the minSingleRegionDepth
	 */
	public int getMinSingleRegionDepth() {
		return minSingleRegionDepth;
	}

	/**
	 * @param minSingleRegionDepth the minSingleRegionDepth to set
	 */
	public void setMinSingleRegionDepth(int minSingleRegionDepth) {
		this.minSingleRegionDepth = minSingleRegionDepth;
	}

	/**
	 * @return the cnvDepth
	 */
	public boolean isCnvDepth() {
		return cnvDepth;
	}

	/**
	 * @param cnvDepth the cnvDepth to set
	 */
	public void setCnvDepth(boolean cnvDepth) {
		this.cnvDepth = cnvDepth;
	}

	/**
	 * @return the genderPredict
	 */
	public boolean isGenderPredict() {
		return genderPredict;
	}

	/**
	 * @param genderPredict the genderPredict to set
	 */
	public void setGenderPredict(boolean genderPredict) {
		this.genderPredict = genderPredict;
	}

	/**
	 * @return the outputUnmapped
	 */
	public boolean isOutputUnmapped() {
		return outputUnmapped;
	}

	/**
	 * @param outputUnmapped the outputUnmapped to set
	 */
	public void setOutputUnmapped(boolean outputUnmapped) {
		this.outputUnmapped = outputUnmapped;
	}

	/**
	 * @return the cnvRegion
	 */
	public String getCnvRegion() {
		return cnvRegion;
	}

	/**
	 * @param cnvRegion the cnvRegion to set
	 */
	public void setCnvRegion(String cnvRegion) {
		this.cnvRegion = cnvRegion;
	}
	
	public boolean isGenderDepth() {
		return getSingleRegion() != null || 
				(getBedfile() != null || getRegion() != null);
	}
}
