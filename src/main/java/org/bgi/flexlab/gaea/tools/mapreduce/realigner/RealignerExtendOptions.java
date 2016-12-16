package org.bgi.flexlab.gaea.tools.mapreduce.realigner;

import org.apache.hadoop.conf.Configuration;
import org.bgi.flexlab.gaea.data.mapreduce.options.HadoopOptions;
import org.bgi.flexlab.gaea.options.GaeaOptions;

public class RealignerExtendOptions  extends GaeaOptions implements HadoopOptions{
	public RealignerExtendOptions(){
		addOption("a", "defaultPlatform", true, "If a read has no platform then default to the provided String."
						+ " Valid options are illumina, 454, and solid.");
		addOption("A", "covariates", true, "One or more(separated by comma) covariates to be used in the recalibration.");
		addOption("b", "bintag", true, "the binary tag covariate name if using it");
		addOption("c","consensusModel",true,"Determines how to compute the possible alternate consenses.model:DBSNP,READS.[READS]");
		addOption("C", "CachedRef", false, "cache reference");
		addOption("d","LOD",true,"LOD threshold above which the cleaner will clean [5.0].");
		addOption("D", "ddq", true, "default quality for the base deletions covariate.(default:45)");
		addOption("e", "windowExtendSize", true, "window extend size[500]");
		addOption("E", "mdq", true, "default quality for the base mismatches covariate.(default:-1)");
		addOption("f", "forcePlatform", true, "If provided, the platform of EVERY read will be forced to be the "
				+ "provided String. Valid options are illumina, 454, and solid.");
		addOption("F", "idq", true, "default quality for the base insertions covariate.（default:45)");
		addOption("g", "mcs", true, "size of the k-mer context to be used for base mismatches.(default:2)");
		addOption("G", "ics", true, "size of the k-mer context to be used for base insertions and deletions.(default:3)");
		addOption("i", "input", true, "input directory", true);
		addOption("I","MaxInsertSize",true,"maximum insert size of read pairs that we attempt to realign [3000].");
		addOption("k", "knowSite", true, "known snp/indel file,the format is VCF4");
		addOption("K", "ls", false, "If specified, just list the available covariates and exit.");
		addOption("l", "minReads", true, "minimum reads at a locus to enable using the entropy calculation[4].");
		addOption("L", "intervalLength", true, "max interval length[500].");
		addOption("m", "maxReadsAtWindows", true, "max reads numbers at on windows[1000000]");
		addOption("M", "multiSample", false, "mutiple sample realignment[false]");
		addOption("n", "reducer", true, "reducer numbers[30]");
		addOption("N", "noStandard", false, "If specified, do not use the standard set of covariates, but rather just the "
				+ "ones listed using the -cov argument.");
		addOption("o", "output", true, "output directory", true);
		addOption("P", "muq", true, "minimum quality for the bases to be preserved.");
		addOption("p", "sMode", true, "How should we recalibrate solid bases in which the reference was inserted? Options = DO_NOTHING, SET_Q_ZERO, "
				+ "SET_Q_ZERO_BASE_N, or REMOVE_REF_BIAS.");
		addOption("Q", "ql", true, "number of distinct quality scores in the quantized output.(default:16)");
		addOption("r", "reference", true, "reference index(generation by GaeaIndex) file path", true);
		addOption("s", "samformat", false, "input file is sam format");
		addOption("S", "solid_nocall_strategy", true, "Defines the behavior of the recalibrator when it encounters no calls in the color space. "
						+ "Options = THROW_EXCEPTION, LEAVE_READ_UNRECALIBRATED, or PURGE_READ.");
		addOption("t", "mismatch", true, "fraction of base qualities needing to mismatch for a position to have high entropy[0]");
		addOption("T", "lqt", true, "minimum quality for the bases in the tail of the reads to be considered.(default:2)");
		addOption("w", "keyWindow", true, "window size for key[10000]");
		addOption("W", "window", true, "window size for calculating entropy or SNP clusters[10]");
	}

	@Override
	public void setHadoopConf(String[] args, Configuration conf) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void getOptionsFromHadoopConf(Configuration conf) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void parse(String[] args) {
		// TODO Auto-generated method stub
		
	}
}
