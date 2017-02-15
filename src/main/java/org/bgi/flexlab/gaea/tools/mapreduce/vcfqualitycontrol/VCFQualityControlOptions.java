package org.bgi.flexlab.gaea.tools.mapreduce.vcfqualitycontrol;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.bgi.flexlab.gaea.data.mapreduce.options.HadoopOptions;
import org.bgi.flexlab.gaea.data.options.GaeaOptions;
import org.bgi.flexlab.gaea.data.structure.header.GaeaVCFHeader;
import org.seqdoop.hadoop_bam.KeyIgnoringVCFOutputFormat;
import org.seqdoop.hadoop_bam.VCFOutputFormat;

public class VCFQualityControlOptions extends GaeaOptions implements HadoopOptions{
	private final static String SOFTWARE_NAME = "VCFQualityControl";
	private final static String SOFTWARE_VERSION = "1.0";
	
	public VCFQualityControlOptions() {
		// TODO Auto-generated constructor stub
		addOption("m", "mode", true, "VQSR:The mode employed to perform vcf quality control(\"1\" is vcf recalibration."
				+ "\"2\" is hard filter. Default:1");
		addOption("i", "input", true, "The raw input variants to be recalibrated.", true);
		addOption("r", "reference", true, "VQSR:reference in fasta format.", true);
		addOption("R", "resource", true, "VQSR:A list of sites for which to apply a prior probability of"
				+ " being correct but which aren't used by the algorithm, separated by ':'.", true);
		addOption("o", "output", true, "The output local path.", true);
		addOption("t", "titv", true, "VQSR:the expected novel Ti/Tv ratio to use when calculating FDR tranches"
				+ " and for display on the optimization curve output figures. (approx 2.15 for whole genome"
				+ " experiments). ONLY USED FOR PLOTTING PURPOSES!");
		addOption("a", "an", true, "VQSR:The names of the annotations which should used for calculations, separated by comma.");
		addOption("T", "TStranche", true, "VQSR:The levels of novel false discovery rate (FDR, implied by ti/tv) at which to slice the data. (in percent, that is 1.0 for 1 percent)");
		addOption("I", "ignoreFilter", true, "VQSR:If specified the variant recalibrator will use variants even if the specified filter name is marked in the input VCF file, separated by comma.");
		addOption("f", "tsFilterLevel", true, "VQSR:The truth sensitivity level at which to start filtering, used here to indicate filtered variants in the model reporting plots");
		addOption("c", "trustAllPolymorphic", false, "VQSR:Trust that all the input training sets' unfiltered records contain only polymorphic sites to drastically speed up the computation.");
		addOption("M", "mode", true, "VQSR:Recalibration mode to employ: 1.) SNP for recalibrating only snps (emitting indels untouched in the output VCF); 2.) INDEL for indels; and 3.) BOTH "
				+ "for recalibrating both snps and indels simultaneously.");
		addOption("g", "maxGaussians", true, "VQSR:The maximum number of Gaussians to try during variational Bayes algorithm");
		addOption("C", "maxIterations", true, "VQSR:The maximum number of VBEM iterations to be performed in variational Bayes algorithm. Procedure will normally end when convergence is detected.");
		addOption("k", "numKMeans", true, "VQSR:The number of k-means iterations to perform in order to initialize the means of the Gaussians in the Gaussian mixture model.");
		addOption("s", "stdThreshold", true, "VQSR:If a variant has annotations more than -std standard deviations away from mean then don't use it for building the Gaussian mixture model.");
		addOption("q", "qualThreshold", true, "VQSR:If a known variant has raw QUAL value less than -qual then don't use it for building the Gaussian mixture model.");
		addOption("G", "shrinkage", true, "VQSR:The shrinkage parameter in the variational Bayes algorithm.");
		addOption("d", "dirichlet", true, "VQSR:The dirichlet parameter in the variational Bayes algorithm.");
		addOption("p", "priorCounts", true, "VQSR:The number of prior counts to use in the variational Bayes algorithm.");
		addOption("P", "percentBadVariants", true, "VQSR:What percentage of the worst scoring variants to use when building the Gaussian mixture model of bad variants. 0.07 means bottom 7 percent.");
		addOption("b", "minNumBadVariants", true, "VQSR:The minimum amount of worst scoring variants to use when building the Gaussian mixture model of bad variants. Will override -percentBad argument if necessary.");
		addOption("O", "hdfsOutputPath", true, "VQSR:hdfs Output Path."
				+ "");
		addOption("l", "tmpPath", true, "VQSR:temp local Path.");
		addOption("H", "hasRscript", true, "VQSR:do Rscript");
		addOption("F", "filterName", true, "Hard Filter:filter name");
		addOption("S", "snpFilter", true, "Hard Filter:snp filter parameter, eg:MQ>10");
		addOption("D", "indelFilter", true, "Hard Filter:indel filter parameter, eg:\"MQ<10||FS>8.0\"\n");
		addOption("h", "help", false, "Display help information.");
	}
	
	/**
	 * vcf quality control mode
	 */
	private int vcfqcMode;
	
	/**
	 * input dir or file
	 */
	private String inputs;
	
	/**
	 * output dir
	 */
	private String outputPath;

	/**
	 * reference for genome Loc
	 */
	private String reference;
	
    /**
     * Any set of VCF files to use as lists of training, truth, or known sites.
     * Training - Input variants which are found to overlap with these training sites are used to build the Gaussian mixture model.
     * Truth - When deciding where to set the cutoff in VQSLOD sensitivity to these truth sites is used.
     * Known - The known / novel status of a variant isn't used by the algorithm itself and is only used for reporting / display purposes.
     * Bad - In addition to using the worst 3% of variants as compared to the Gaussian mixture model, we can also supplement the list with a database of known bad variants.
     */
	private List<String> resources;

    /**
     * The expected transition / tranversion ratio of true novel variants in your targeted region (whole genome, exome, specific
     * genes), which varies greatly by the CpG and GC content of the region. See expected Ti/Tv ratios section of the GATK best
     * practices wiki documentation for more information. Normal whole genome values are 2.15 and for whole exome 3.2. Note
     * that this parameter is used for display purposes only and isn't used anywhere in the algorithm!
     */
	private double TargetTiTv;

    /**
     * See the input VCF file's INFO field for a list of all available annotations.
     */
	private List<String> UseAnnotatitions;

    /**
     * Add truth sensitivity slices through the call set at the given values. The default values are 100.0, 99.9, 99.0, and 90.0
     * which will result in 4 estimated tranches in the final call set: the full set of calls (100% sensitivity at the accessible
     * sites in the truth set), a 99.9% truth sensitivity tranche, along with progressively smaller tranches at 99% and 90%.
     */
	private String TsTranches;
	
	/**
	 * keep vcf varition if the filters are in this
	 */
	private HashSet<String> IgnoreInputFilters;
    
    /**
     * Tranches filter level
     */
	private double TSFilterLevel;

   /**
    * trust all Polymorphic
    */
	private boolean TrustAllPolymorphic; 
	
	/**
	 * Recalibration mode to employ: 1.) SNP for recalibrating only snps (emitting indels untouched in the output VCF); 
	 * 2.) INDEL for indels; and 3.) BOTH for recalibrating both snps and indels simultaneously.
	 */
	private Mode mode;
	 
	/**
	 * The maximum number of Gaussians to try during variational Bayes algorithm
	 */
	private int maxGaussians;
	 
	/**
	 * The maximum number of VBEM iterations to be performed in variational Bayes algorithm.
	 * Procedure will normally end when convergence is detected.
	 */
	private int maxIterations;
	 
	/**
	 * The number of k-means iterations to perform in order to initialize the means of the Gaussians in the Gaussian mixture model.
	 */
	private int numKMeansIterations;
	 
	/**
	 * If a variant has annotations more than -std standard deviations away from mean then don't use it for building the Gaussian mixture model.
	 */
	private double stdThreshold;
	 
	/**
	 * If a known variant has raw QUAL value less than -qual then don't use it for building the Gaussian mixture model.
	 */
	private double qualThreshold;
	 
	/**
	 * The shrinkage parameter in the variational Bayes algorithm.
	 */
	private double shrinkage;
	 
	/**
	 * The dirichlet parameter in the variational Bayes algorithm.
	 */
	private double dirichletParamenter;
	 
	/**
	 * The number of prior counts to use in the variational Bayes algorithm.
	 */
	private double priorCounts;
	 
	/**
	 * What percentage of the worst scoring variants to use when building the Gaussian mixture model of bad variants. 0.07 means bottom 7 percent.
	 */
	private double percentBadVariants;
	 
	/**
	 * The minimum amount of worst scoring variants to use when building the Gaussian mixture model of bad variants. Will override -percentBad argument if necessary.
	 */
	private int minNumBadVariants;
	
	private List<Path> inputList;
	
	private String filterName;
	
	private String snpFilter;
	
	private String indelFilter;
	
	/**
	 * class of Model
	 * @author zhangyong2
	 *
	 */
	public enum Mode {
        SNP("SNP"),
        INDEL("INDEL"),
        BOTH("BOTH");
        
        private String mode;
        Mode(String mode) {
        	this.mode = mode;
        }
        public String getModeString() {
        	return mode;
        }
	}
	
	@Override
	public void setHadoopConf(String[] args, Configuration conf) {
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
		
		vcfqcMode = getOptionIntValue("m", 1);
		
		inputs = getOptionValue("i", null);
		
		outputPath = getOptionValue("o", null);
		
		
		if(vcfqcMode == 1) {
			try {
				reference = getOptionValue("r", null);
				
				String resource = getOptionValue("R", null); 
				resources.addAll(Arrays.asList(resource.split(":")));
				
				outputPath = getOptionValue("o", null);
							
				TargetTiTv = getOptionDoubleValue("t", 2.15);
				
				String an = getOptionValue("a", null);
				UseAnnotatitions.addAll(Arrays.asList(an.split(",")));
				
				TsTranches = getOptionValue("T", null);
				
				String ignoreFilters = getOptionValue("I", null);
				IgnoreInputFilters.addAll(Arrays.asList(ignoreFilters.split(",")));
				
				TSFilterLevel = getOptionDoubleValue("f", 99.0);
				
				TrustAllPolymorphic = getOptionBooleanValue("c", false);
				
				mode = Mode.valueOf((getOptionValue("m", "SNP")));
				
				maxGaussians = getOptionIntValue("g", 10);
				
				maxIterations = getOptionIntValue("C", 100);
				
				numKMeansIterations = getOptionIntValue("k", 30);
				
				stdThreshold = getOptionDoubleValue("s", 14.0);
				
				qualThreshold = getOptionDoubleValue("q", 80.0);
				
				shrinkage = getOptionDoubleValue("G", 1.0);
				
				dirichletParamenter = getOptionDoubleValue("d", 0.001);
				
				priorCounts = getOptionDoubleValue("p", 20.0);
				
				percentBadVariants = getOptionDoubleValue("P", 0.03);
				
				minNumBadVariants = getOptionIntValue("b", 2500);
			} catch(Exception e) {
				throw new IllegalArgumentException("Unrecognized parameter for VQSR, please refer to the help information");
			}
		} else if(vcfqcMode == 2) {
			try{
				filterName = getOptionValue("F", null);
				
				snpFilter = getOptionValue("S", null);
				
				indelFilter = getOptionValue("D", null);
			} catch(Exception e) {
				throw new IllegalArgumentException("unrecognized parameter for Hard Filter, please refer to the help information");
			}
		} else {
			throw new RuntimeException("Unvalid mode is employed. Please refer to the help information");
		}
	}
	
	/**
	 * @return the inputs
	 */
	public String getInputs() {
		return inputs;
	}

	/**
	 * @param inputs the inputs to set
	 */
	public void setInputs(String inputs) {
		this.inputs = inputs;
	}

	/**
	 * @return the resources
	 */
	public List<String> getResources() {
		return resources;
	}
	
	/**
	 * @param resources the resources to set
	 */
	public void setResources(List<String> resources) {
		this.resources = resources;
	}

	/**
	 * @return the recalFile
	 */
	public String getOutputPath() {
		return outputPath;
	}

	/**
	 * @param recalFile the recalFile to set
	 */
	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}
	
	/**
	 * @return the targetTiTv
	 */
	public double getTargetTiTv() {
		return TargetTiTv;
	}

	/**
	 * @param targetTiTv the targetTiTv to set
	 */
	public void setTargetTiTv(double targetTiTv) {
		TargetTiTv = targetTiTv;
	}

	/**
	 * @return the useAnnotatitions
	 */
	public List<String> getUseAnnotations() {
		return UseAnnotatitions;
	}

	/**
	 * @param useAnnotatitions the useAnnotatitions to set
	 */
	public void setUseAnnotatitions(List<String> useAnnotatitions) {
		UseAnnotatitions = useAnnotatitions;
	}
	
	public void traversalInputPath(Path path) {
		Configuration conf = new Configuration();
		FileSystem fs = null;
		try {
			fs = path.getFileSystem(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			if (!fs.exists(path)) {
				System.err
						.println("Input File Path is not exist! Please check -I var.");
				System.exit(-1);
			}
			if (fs.isFile(path)) {
				inputList.add(path);
			} else {
				FileStatus stats[] = fs.listStatus(path);

				for (FileStatus file : stats) {
					Path filePath = file.getPath();

					if (!fs.isFile(filePath)) {
						traversalInputPath(filePath);
					} else {
						inputList.add(filePath);
					}
				}
			}
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}
	}
	
	public double[] getTsTranchesDouble() {
		double[] TsTranchesDouble = new double[] {100.0, 99.9, 99.0, 90.0};
		if(TsTranches != null && TsTranches != "") {
			String[] lineSplit  = TsTranches.split(",");
			
			if(lineSplit.length != 4) {
				throw new RuntimeException("wrong TStranche number, should be 4 double number, split by ','!");
			}
			
			for(int i = 0; i < 4; i++) {
				TsTranchesDouble[i] = Double.parseDouble(lineSplit[i]);
			}
		}
		return TsTranchesDouble;
	}
	
	
	/**
	 * @return the tsTranches
	 */
	public String getTsTranches() {
		return TsTranches;
	}

	/**
	 * @param tsTranches the tsTranches to set
	 */
	public void setTsTranches(String tsTranches) {
		TsTranches = tsTranches;
	}

	/**
	 * @return the ignoreInputeFilters
	 */
	public HashSet<String> getIgnoreInputFilters() {
		return IgnoreInputFilters;
	}

	/**
	 * @param ignoreInputeFilters the ignoreInputeFilters to set
	 */
	public void setIgnoreInputeFilters(HashSet<String> ignoreInputeFilters) {
		IgnoreInputFilters = ignoreInputeFilters;
	}

	/**
	 * @return the tSFilterLevel
	 */
	public double getTSFilterLevel() {
		return TSFilterLevel;
	}

	/**
	 * @param tSFilterLevel the tSFilterLevel to set
	 */
	public void setTSFilterLevel(double tSFilterLevel) {
		TSFilterLevel = tSFilterLevel;
	}

	/**
	 * @return the trustAllPolymorphic
	 */
	public boolean isTrustAllPolymorphic() {
		return TrustAllPolymorphic;
	}

	/**
	 * @param trustAllPolymorphic the trustAllPolymorphic to set
	 */
	public void setTrustAllPolymorphic(boolean trustAllPolymorphic) {
		TrustAllPolymorphic = trustAllPolymorphic;
	}

	/**
	 * @return the mode
	 */
	public Mode getMode() {
		return mode;
	}

	/**
	 * @param mode the mode to set
	 */
	public void setMode(Mode mode) {
		this.mode = mode;
	}

	/**
	 * @return the maxGaussians
	 */
	public int getMaxGaussians() {
		return maxGaussians;
	}

	/**
	 * @param maxGaussians the maxGaussians to set
	 */
	public void setMaxGaussians(int maxGaussians) {
		this.maxGaussians = maxGaussians;
	}

	/**
	 * @return the maxIterations
	 */
	public int getMaxIterations() {
		return maxIterations;
	}

	/**
	 * @param maxIterations the maxIterations to set
	 */
	public void setMaxIterations(int maxIterations) {
		this.maxIterations = maxIterations;
	}

	/**
	 * @return the numKMeansIterations
	 */
	public int getNumKMeansIterations() {
		return numKMeansIterations;
	}

	/**
	 * @param numKMeansIterations the numKMeansIterations to set
	 */
	public void setNumKMeansIterations(int numKMeansIterations) {
		this.numKMeansIterations = numKMeansIterations;
	}

	/**
	 * @return the stdThreshold
	 */
	public double getStdThreshold() {
		return stdThreshold;
	}

	/**
	 * @param stdThreshold the stdThreshold to set
	 */
	public void setStdThreshold(double stdThreshold) {
		this.stdThreshold = stdThreshold;
	}

	/**
	 * @return the qualThreshold
	 */
	public double getQualThreshold() {
		return qualThreshold;
	}

	/**
	 * @param qualThreshold the qualThreshold to set
	 */
	public void setQualThreshold(double qualThreshold) {
		this.qualThreshold = qualThreshold;
	}

	/**
	 * @return the shrinkage
	 */
	public double getShrinkage() {
		return shrinkage;
	}

	/**
	 * @param shrinkage the shrinkage to set
	 */
	public void setShrinkage(double shrinkage) {
		this.shrinkage = shrinkage;
	}

	/**
	 * @return the dirichletParamenter
	 */
	public double getDirichletParamenter() {
		return dirichletParamenter;
	}

	/**
	 * @param dirichletParamenter the dirichletParamenter to set
	 */
	public void setDirichletParamenter(double dirichletParamenter) {
		this.dirichletParamenter = dirichletParamenter;
	}

	/**
	 * @return the priorCounts
	 */
	public double getPriorCounts() {
		return priorCounts;
	}

	/**
	 * @param priorCounts the priorCounts to set
	 */
	public void setPriorCounts(double priorCounts) {
		this.priorCounts = priorCounts;
	}

	/**
	 * @return the percentBadVariants
	 */
	public double getPercentBadVariants() {
		return percentBadVariants;
	}

	/**
	 * @param percentBadVariants the percentBadVariants to set
	 */
	public void setPercentBadVariants(double percentBadVariants) {
		this.percentBadVariants = percentBadVariants;
	}

	/**
	 * @return the minNumBadVariants
	 */
	public int getMinNumBadVariants() {
		return minNumBadVariants;
	}

	/**
	 * @param minNumBadVariants the minNumBadVariants to set
	 */
	public void setMinNumBadVariants(int minNumBadVariants) {
		this.minNumBadVariants = minNumBadVariants;
	}

	/**
	 * @return the reference
	 */
	public String getReference() {
		return reference;
	}

	/**
	 * @param reference the reference to set
	 */
	public void setReference(String reference) {
		this.reference = reference;
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
	
	public List<Path> getInputFiles() {
		traversalInputPath(new Path(inputs));
		return inputList;
	}
	
	public String getFilterName() {
		return filterName;
	}
	
	public String setFilterName() {
		return filterName;
	}
	
	public boolean isRecal() {
		return vcfqcMode == 1;
	}
	
}
