package org.bgi.flexlab.gaea.tools.mapreduce.realigner;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.bgi.flexlab.gaea.data.mapreduce.options.HadoopOptions;
import org.bgi.flexlab.gaea.data.options.GaeaOptions;

public class RealignerExtendOptions extends GaeaOptions implements HadoopOptions {
	public final static String SOFTWARE_NAME = "Realigner";
	public final static String SOFTWARE_VERSION = "1.0";

	private RealignerOptions realignerOptions = new RealignerOptions();
	private RecalibratorOptions bqsrOptions = new RecalibratorOptions();

	private boolean realignment;
	private boolean recalibration;

	public RealignerExtendOptions() {
		addOption("a", "defaultPlatform", true, "If a read has no platform then default to the provided String."
				+ " Valid options are illumina, 454, and solid.");
		addOption("A", "covariates", true,
				"One or more(separated by comma) covariates to be used in the recalibration.");
		addOption("b", "bintag", true, "the binary tag covariate name if using it");
		addOption("c", "consensusModel", true,
				"Determines how to compute the possible alternate consenses.model:DBSNP,READS.[READS]");
		addOption("C", "CachedRef", false, "cache reference");
		addOption("d", "LOD", true, "LOD threshold above which the cleaner will clean [5.0].");
		addOption("D", "ddq", true, "default quality for the base deletions covariate.(default:45)");
		addOption("e", "windowExtendSize", true, "window extend size[500]");
		addOption("E", "mdq", true, "default quality for the base mismatches covariate.(default:-1)");
		addOption("f", "forcePlatform", true, "If provided, the platform of EVERY read will be forced to be the "
				+ "provided String. Valid options are illumina, 454, and solid.");
		addOption("F", "idq", true, "default quality for the base insertions covariate.（default:45)");
		addOption("g", "mcs", true, "size of the k-mer context to be used for base mismatches.(default:2)");
		addOption("G", "ics", true,
				"size of the k-mer context to be used for base insertions and deletions.(default:3)");
		addOption("i", "input", true, "input directory", true);
		addOption("I", "MaxInsertSize", true, "maximum insert size of read pairs that we attempt to realign [3000].");
		addOption("k", "knowSite", true, "known snp/indel file,the format is VCF4");
		addOption("K", "ls", false, "If specified, just list the available covariates and exit.");
		addOption("l", "minReads", true, "minimum reads at a locus to enable using the entropy calculation[4].");
		addOption("L", "intervalLength", true, "max interval length[500].");
		addOption("m", "maxReadsAtWindows", true, "max reads numbers at on windows[1000000]");
		addOption("M", "multiSample", false, "mutiple sample realignment[false]");
		addOption("n", "reducer", true, "reducer numbers[30]");
		addOption("N", "noStandard", false,
				"If specified, do not use the standard set of covariates, but rather just the "
						+ "ones listed using the -cov argument.");
		addOption("o", "output", true, "output directory", true);
		addOption("p", "muq", true, "minimum quality for the bases to be preserved.");
		addOption("P", "sMode", true,
				"How should we recalibrate solid bases in which the reference was inserted? Options = DO_NOTHING, SET_Q_ZERO, "
						+ "SET_Q_ZERO_BASE_N, or REMOVE_REF_BIAS.");
		addOption("q", "recalibrator", false, "only run bqsr");
		addOption("Q", "ql", true, "number of distinct quality scores in the quantized output.(default:16)");
		addOption("r", "reference", true, "reference index(generation by GaeaIndex) file path", true);
		addOption("R", "realigment", false, "only run realiger");
		addOption("s", "samformat", false, "input file is sam format");
		addOption("S", "solid_nocall_strategy", true,
				"Defines the behavior of the recalibrator when it encounters no calls in the color space. "
						+ "Options = THROW_EXCEPTION, LEAVE_READ_UNRECALIBRATED, or PURGE_READ.");
		addOption("t", "mismatch", true,
				"fraction of base qualities needing to mismatch for a position to have high entropy[0]");
		addOption("T", "lqt", true,
				"minimum quality for the bases in the tail of the reads to be considered.(default:2)");
		addOption("u", "algoBoth", false, "run realiger and recalibrator");
		addOption("w", "keyWindow", true, "window size for key[10000]");
		addOption("W", "window", true, "window size for calculating entropy or SNP clusters[10]");

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
		ArrayList<String> realigner = new ArrayList<String>();
		ArrayList<String> bqsr = new ArrayList<String>();
		
		HashMap<String,Boolean> realignerShortOptions = realignerOptions.getShortOptionSet();
		HashMap<String,Boolean> bqsrShortOptions = bqsrOptions.getShortOptionSet();

		try {
			cmdLine = parser.parse(options, args);
		} catch (ParseException e) {
			System.err.println(e.getMessage());
			FormatHelpInfo(SOFTWARE_NAME, SOFTWARE_VERSION);
			System.exit(1);
		}
		
		if(cmdLine.hasOption("u")){
			realignment = recalibration = true;
		}else{
			if(cmdLine.hasOption("q"))
				recalibration = true;
			if(cmdLine.hasOption("R"))
				realignment = true;
		}
		
		if (!isValid()) {
			throw new RuntimeException("must set at least one algorithm!");
		}
		
		for(String op : realignerShortOptions.keySet()){
			if(cmdLine.hasOption(op)){
				realigner.add("-"+op);
				if(realignerShortOptions.get(op))
					realigner.add(cmdLine.getOptionValue(op));
			}
		}
		
		for(String op : bqsrShortOptions.keySet()){
			if(cmdLine.hasOption(op)){
				bqsr.add("-"+op);
				if(bqsrShortOptions.get(op))
					bqsr.add(cmdLine.getOptionValue(op));
			}
		}
		
		realignerOptions.parse((String[])realigner.toArray(new String[realigner.size()]));
		bqsrOptions.parse((String[])bqsr.toArray(new String[bqsr.size()]));
		
		realigner.clear();
		bqsr.clear();
	}

	private boolean isValid() {
		return realignment && recalibration;
	}

	public boolean isRealignment() {
		return this.realignment;
	}

	public boolean isRecalibration() {
		return this.recalibration;
	}

	public RealignerOptions getRealignerOptions() {
		return this.realignerOptions;
	}

	public RecalibratorOptions getBqsrOptions() {
		return this.bqsrOptions;
	}
}
