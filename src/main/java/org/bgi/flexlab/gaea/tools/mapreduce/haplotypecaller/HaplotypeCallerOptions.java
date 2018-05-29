package org.bgi.flexlab.gaea.tools.mapreduce.haplotypecaller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.bgi.flexlab.gaea.data.exception.UserException;
import org.bgi.flexlab.gaea.data.mapreduce.options.HadoopOptions;
import org.bgi.flexlab.gaea.data.options.GaeaOptions;
import org.bgi.flexlab.gaea.tools.haplotypecaller.ReferenceConfidenceMode;
import org.bgi.flexlab.gaea.tools.haplotypecaller.argumentcollection.HaplotypeCallerArgumentCollection;
import org.bgi.flexlab.gaea.tools.mapreduce.realigner.RealignerExtendOptions;
import org.seqdoop.hadoop_bam.SAMFormat;

public class HaplotypeCallerOptions  extends GaeaOptions implements HadoopOptions{
	
	private final static String SOFTWARE_NAME = "HaplotypeCaller";
	
	private final static String SOFTWARE_VERSION = "1.0";
	
	private String region = null;
	
	private String reference = null;
	
	private int windowsSize = 10000;
	
	private int windowsExtends = 300;
	
	private int readShardSize = 300;
	
	private int readPaddingSize = 100;
	
	private List<String> userDisabledReadFilterNames = new ArrayList<>();
	
	private List<String> userEnabledReadFilterNames = new ArrayList<>();

	private  boolean disableToolDefaultReadFilters = false;

	private String dbsnp = null;
	
	private String allele = null;
	
	private boolean gvcfFormat = false;
	
	private HaplotypeCallerArgumentCollection hcArgs = new HaplotypeCallerArgumentCollection();
	
	private int reduceNumber = 100;
	
	private List<Path> inputs = new ArrayList<Path>();
	
	private String output = null;
	
	private boolean isSAM = false;
	
	private HashMap<String,String> comps  = new HashMap<String,String>();
	
	private int maxReadsPerPosition = 0;
	
	public HaplotypeCallerOptions() {
		addOption("a","allSitePLs",false,"Annotate all sites with PLs");
		addOption("A","annotateNDA",false,"If provided, we will annotate records with the number of alternate alleles that were discovered (but not necessarily genotyped) at a given site");
		addOption("b","hets",true,"Heterozygosity value used to compute prior likelihoods for any locus");
		addOption("B","indel_hets",true,"Heterozygosity for indel calling");
		addOption("C","sample_ploidy",true,"Ploidy (number of chromosomes) per sample. For pooled data, set to (Number of samples in each pool * Sample Ploidy).");
		addOption("c","shard_size",true,"read shard size.");
		addOption("d","shard_padding_size",true,"read shard padding size.");
		addOption("D","max_reads",true,"max reads for pileup.");
		addOption("E","windowExtendSize",true,"key window extend size.");
		addOption("e","max_depth_for_assembly",true,"max depth for assembly.");
		addOption("f", "format", false, "output format is gvcf");
		addOption("G", "gt_mode",true,"Specifies how to determine the alternate alleles to use for genotyping(DISCOVERY or GENOTYPE_GIVEN_ALLELES)");
		addOption("i", "input", true, "a bam or bam list for input", true);
		addOption("I","include_non_variant",false,"Include loci found to be non-variant after genotyping");
		addOption("j","heterozygosity_stdev",true,"Standard deviation of eterozygosity for SNP and indel calling");
		addOption("k", "knowSite", true, "known snp/indel file,the format is VCF4");
		addOption("n", "reducer", true, "reducer numbers[100]");
		addOption("m","max_num_PL_values",true,"Maximum number of PL values to output");
		addOption("M","max_alternate_alleles",true,"Maximum number of alternate alleles to genotype");
		addOption("o", "output", true, "output directory", true);
		addOption("O","output_mode",true,"output mode(EMIT_VARIANTS_ONLY,EMIT_ALL_CONFIDENT_SITES,EMIT_ALL_SITES)");
		addOption("p","input_prior",true,"Input prior for calls(separation by Comma(,))");
		addOption("r", "reference", true, "reference index(generation by GaeaIndex) file path", true);
		addOption("R", "region", true, "One or more genomic intervals over which to operate");
		addOption("s","stand_emit_conf",true,"The minimum phred-scaled confidence threshold at which variants should be emitted (and filtered with LowQual if less than the calling threshold");
		addOption("S","stand_call_conf",true,"The minimum phred-scaled confidence threshold at which variants should be called");
		addOption("u","uniquifySamples",false,"Assume duplicate samples are present and uniquify all names with '.variant' and file number index");
		addOption("U","useNewAFCalculator",false,"Use new AF model instead of the so-called exact model");
		addOption("w", "keyWindow", true, "window size for key[10000]");
		FormatHelpInfo(SOFTWARE_NAME,SOFTWARE_VERSION);
	}
	
	private void initializeReadFilter() {
		
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
			FormatHelpInfo(RealignerExtendOptions.SOFTWARE_NAME, RealignerExtendOptions.SOFTWARE_VERSION);
			System.exit(1);
		}
		
		try {
			parseInput( getOptionValue("i",null));
		} catch (IOException e) {
			throw new UserException(e.toString());
		}
		
		if(getOptionBooleanValue("f",false)){
			this.hcArgs.emitReferenceConfidence = ReferenceConfidenceMode.GVCF;
		}
		
		this.hcArgs.maxDepthForAssembly = getOptionIntValue("e",0);
		
		this.windowsSize = getOptionIntValue("w",10000);
		this.reduceNumber = getOptionIntValue("n",100);
		this.readShardSize = getOptionIntValue("c",300);
		this.readPaddingSize = getOptionIntValue("d",100);
		this.maxReadsPerPosition = getOptionIntValue("D",0);
		
		this.output = getOptionValue("o",null);
		this.reference = getOptionValue("r",null);
		this.dbsnp = getOptionValue("k",null);
		
		if(dbsnp != null) {
			comps.put("DB", dbsnp);
		}
	}
	
	private void parseInput(String input) throws IOException {
		Path path = new Path(input);
		SAMFormat fmt = SAMFormat.inferFromData(path.getFileSystem(new Configuration()).open(path));
		
		if(fmt == SAMFormat.BAM)
			inputs.add(path);
		else {
			LineReader reader = new LineReader(path.getFileSystem(new Configuration()).open(path));
			Text line = new Text();
			
			while(reader.readLine(line) > 0 && line.getLength() != 0) {
				inputs.add(new Path(line.toString()));
			}
			reader.close();
		}
	}

	public String getRegion(){
		return this.region;
	}
	
	public String getReference(){
		return this.reference;
	}
	
	public int getWindowSize(){
		return windowsSize;
	}
	
	public int getWindowsExtendSize(){
		return windowsExtends;
	}
	
	public int getReadShardSize() {
		return this.readShardSize;
	}
	
	public int getReadShardPadding() {
		return this.readPaddingSize;
	}
	
	public List<String> getUserDisabledReadFilterNames(){
		return this.userDisabledReadFilterNames;
	}
	
	public List<String> getUserEnabledReadFilterNames(){
		return this.userEnabledReadFilterNames;
	}
	
	public boolean getDisableToolDefaultReadFilters() {
		return this.disableToolDefaultReadFilters;
	}
	
	public String getDBSnp() {
		return dbsnp;
	}
	
	public String getAlleleFile() {
		return this.allele;
	}
	
	public boolean isGVCF() {
		return this.gvcfFormat;
	}
	
	public int getReducerNumber() {
		return this.reduceNumber;
	}
	
	public List<Path> getInput(){
		return this.inputs;
	}
	
	public String getHeaderOutput() {
		return this.output;
	}
	
	public String getVCFOutput() {
		if(!output.endsWith("/"))
			output += "/";
		return output+"vcf";
	}
	
	public SAMFormat getInputFormat() {
		if(isSAM)
			return SAMFormat.SAM;
		return SAMFormat.BAM;
	}
	
	public HaplotypeCallerArgumentCollection getHaplotypeCallerArguments() {
		return this.hcArgs;
	}
	
	public List<String> getCompNames(){
		List<String> list = new ArrayList<String>();
		
		for(String name : comps.keySet())
			list.add(name);
		
		return list;
	}
	
	public int getMaxReadsPerPosition(){
		return this.maxReadsPerPosition;
	}
}
