package org.bgi.flexlab.gaea.tools.mapreduce.recalibrator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.bgi.flexlab.gaea.data.mapreduce.options.HadoopOptions;
import org.bgi.flexlab.gaea.options.GaeaOptions;

public class BaseRecalibrationOptions extends GaeaOptions implements HadoopOptions {
	private final static String SOFTWARE_NAME = "BaseRecalibration";
	private final static String SOFTWARE_VERSION = "1.0";
	
	public BaseRecalibrationOptions() {
		addOption("i", "input", true, "input bam or bams or sam(separated by comma)", true);
		addOption("t", "inputType", true, "bam:0 sam:1(default:0");
		addOption("o", "output", true, "output path", true);
		addOption("T", "programType", true, "program type.[ALL|BR|CM].BR:run base recalibration in "
				+ "hadoop.CM:combinar the hadoop result  as one table.(default:ALL)");
		addOption("r", "reference", true, "reference", true);
		addOption("k", "knownSites", true, "know  variant site,for example dbsnp!", true);
		addOption("b", "bintag", true, "the binary tag covariate name if using it");
		addOption("d", "defaultPlatform", true, "If a read has no platform then default to the provided String."
				+ " Valid options are illumina, 454, and solid.");
		addOption("f", "forcePlatform", true, "If provided, the platform of EVERY read will be forced to be the "
				+ "provided String. Valid options are illumina, 454, and solid.");
		addOption("c", "covariates", true, "One or more(separated by comma) covariates to be used in the recalibration.");
		addOption("e", "mcs", true, "size of the k-mer context to be used for base mismatches.(default:2)");
		addOption("f", "ics", true, "size of the k-mer context to be used for base insertions and deletions.(default:3)");
		addOption("E", "mdq", true, "default quality for the base mismatches covariate.(default:-1)");
		addOption("F", "idq", true, "default quality for the base insertions covariate.（default:45)");
		addOption("D", "ddq", true, "default quality for the base deletions covariate.(default:45)");
		addOption("L", "lqt", true, "minimum quality for the bases in the tail of the reads to be considered.(default:2)");
		addOption("P", "muq", true, "minimum quality for the bases to be preserved.");
		addOption("Q", "ql", true, "number of distinct quality scores in the quantized output.(default:16)");
		addOption("n", "reducerNumber", true, "number of reducer.(default:30)");
		addOption("w", "winSize", true, "window size.(default:10000)");
		addOption("l", "ls", false, "If specified, just list the available covariates and exit.");
		addOption("N", "noStandard", false, "If specified, do not use the standard set of covariates, but rather just the "
				+ "ones listed using the -cov argument.");
		addOption("p", "sMode", true, "How should we recalibrate solid bases in which the reference was inserted? Options = DO_NOTHING, SET_Q_ZERO, "
				+ "SET_Q_ZERO_BASE_N, or REMOVE_REF_BIAS.");
		addOption("s", "solid_nocall_strategy", true, "Defines the behavior of the recalibrator when it encounters no calls in the color space. "
				+ "Options = THROW_EXCEPTION, LEAVE_READ_UNRECALIBRATED, or PURGE_READ.");
		addOption("C", "CachedRef", false, "cache reference");
		addOption("m", "MultiSample", false, "multiple sample list");
		addOption("h", "help", false, "help information");
	}

	public boolean LIST_ONLY = false;

	public String[] COVARIATES = null;

	public boolean DO_NOT_USE_STANDARD_COVARIATES = false;

	public RecalUtils.SOLID_RECAL_MODE SOLID_RECAL_MODE = RecalUtils.SOLID_RECAL_MODE.SET_Q_ZERO;

	public RecalUtils.SOLID_NOCALL_STRATEGY SOLID_NOCALL_STRATEGY = RecalUtils.SOLID_NOCALL_STRATEGY.THROW_EXCEPTION;

	public int MISMATCHES_CONTEXT_SIZE;

	public int INDELS_CONTEXT_SIZE;

	public byte MISMATCHES_DEFAULT_QUALITY;

	public byte INSERTIONS_DEFAULT_QUALITY;

	public byte DELETIONS_DEFAULT_QUALITY;

	public byte LOW_QUAL_TAIL;

	public int QUANTIZING_LEVELS;

	public int PRESERVE_QSCORES_LESS_THAN;

	public String BINARY_TAG_NAME = null;

	public String DEFAULT_PLATFORM = null;
	
	public String FORCE_PLATFORM = null;
	
	public boolean KEEP_INTERMEDIATE_FILES = false;
	
	public boolean NO_PLOTS = false;
	
	private List<String> knownSites = null;

	private String tempPath = null;

	private String output;
	
	private String input;
	
	private ArrayList<Path> inputList = new ArrayList<Path>();
	
	private String reference;
	
	private int winSize;
	
	private int reducerN;
	
	private int inputType;

	private String TYPE;
	
	private boolean isCachedRef;
	
	private boolean multiSample;
	
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

		input = getOptionValue("i", null);
		traversalInputPath(new Path(input));
		
		output = getOptionValue("o", null);
		
		inputType = getOptionIntValue("t", 0);
		
		reference = getOptionValue("r", null);
		
		if (knownSites == null)
			knownSites = new ArrayList<String>();
		this.knownSites.add(getOptionValue("k", null));
		
		BINARY_TAG_NAME = getOptionValue("b", null);
		
		DEFAULT_PLATFORM = getOptionValue("d", null);
		
		FORCE_PLATFORM = getOptionValue("f", null);
		
		COVARIATES = getOptionValue("c", null) == null ? null 
				: getOptionValue("c", null).split(",");
		
		MISMATCHES_CONTEXT_SIZE = getOptionIntValue("e", 2);
		
		INDELS_CONTEXT_SIZE = getOptionIntValue("f", 3);
		
		MISMATCHES_DEFAULT_QUALITY = (byte) getOptionIntValue("E", -1);
		
		INSERTIONS_DEFAULT_QUALITY = (byte) getOptionIntValue("F", 45);
		
		DELETIONS_DEFAULT_QUALITY = (byte) getOptionIntValue("D", 45);
		
		LOW_QUAL_TAIL = (byte) getOptionIntValue("L", 2);
		
		PRESERVE_QSCORES_LESS_THAN = getOptionIntValue("P", QualityUtils.MIN_USABLE_Q_SCORE);
		
		QUANTIZING_LEVELS = getOptionIntValue("Q", 16);
		
		reducerN = getOptionIntValue("n", 30);
		
		winSize = getOptionIntValue("w", 1000000);
		
		LIST_ONLY = getOptionBooleanValue("l", false);
		
		DO_NOT_USE_STANDARD_COVARIATES = getOptionBooleanValue("N", false);
		
		SOLID_RECAL_MODE = getOptionValue("P", null) == null ? RecalUtils.SOLID_RECAL_MODE.SET_Q_ZERO 
				: setSOLID_RECAL_MODE(getOptionValue("p", null));
		
		SOLID_NOCALL_STRATEGY = getOptionValue("P", null) == null ? RecalUtils.SOLID_NOCALL_STRATEGY.THROW_EXCEPTION 
				: setSOLID_NOCALL_STRATEGY(getOptionValue("s", null));
		
		TYPE = getOptionValue("T", "ALL");
		
		isCachedRef = getOptionBooleanValue("C", false);
		
		multiSample = getOptionBooleanValue("m", false);
	}

	public ReportTable generateReportTable(final String covariateNames) {
		ReportTable argumentsTable = new ReportTable("Arguments",
				"Recalibration argument collection values used in this run", 2);
		argumentsTable.addColumn("Argument");
		argumentsTable.addColumn(RecalUtils.ARGUMENT_VALUE_COLUMN_NAME);
		argumentsTable.addRowID("covariate", true);
		argumentsTable.set("covariate", RecalUtils.ARGUMENT_VALUE_COLUMN_NAME,
				covariateNames);
		argumentsTable.addRowID("no_standard_covs", true);
		argumentsTable.set("no_standard_covs",
				RecalUtils.ARGUMENT_VALUE_COLUMN_NAME,
				DO_NOT_USE_STANDARD_COVARIATES);
		argumentsTable.addRowID("run_without_dbsnp", true);
		argumentsTable.addRowID("solid_recal_mode", true);
		argumentsTable.set("solid_recal_mode",
				RecalUtils.ARGUMENT_VALUE_COLUMN_NAME, SOLID_RECAL_MODE);
		argumentsTable.addRowID("solid_nocall_strategy", true);
		argumentsTable.set("solid_nocall_strategy",
				RecalUtils.ARGUMENT_VALUE_COLUMN_NAME, SOLID_NOCALL_STRATEGY);
		argumentsTable.addRowID("mismatches_context_size", true);
		argumentsTable.set("mismatches_context_size",
				RecalUtils.ARGUMENT_VALUE_COLUMN_NAME, MISMATCHES_CONTEXT_SIZE);
		argumentsTable.addRowID("indels_context_size", true);
		argumentsTable.set("indels_context_size",
				RecalUtils.ARGUMENT_VALUE_COLUMN_NAME, INDELS_CONTEXT_SIZE);
		argumentsTable.addRowID("mismatches_default_quality", true);
		argumentsTable.set("mismatches_default_quality",
				RecalUtils.ARGUMENT_VALUE_COLUMN_NAME,
				MISMATCHES_DEFAULT_QUALITY);
		argumentsTable.addRowID("insertions_default_quality", true);
		argumentsTable.set("insertions_default_quality",
				RecalUtils.ARGUMENT_VALUE_COLUMN_NAME,
				INSERTIONS_DEFAULT_QUALITY);
		argumentsTable.addRowID("low_quality_tail", true);
		argumentsTable.set("low_quality_tail",
				RecalUtils.ARGUMENT_VALUE_COLUMN_NAME, LOW_QUAL_TAIL);
		argumentsTable.addRowID("default_platform", true);
		argumentsTable.set("default_platform",
				RecalUtils.ARGUMENT_VALUE_COLUMN_NAME, DEFAULT_PLATFORM);
		argumentsTable.addRowID("force_platform", true);
		argumentsTable.set("force_platform",
				RecalUtils.ARGUMENT_VALUE_COLUMN_NAME, FORCE_PLATFORM);
		argumentsTable.addRowID("quantizing_levels", true);
		argumentsTable.set("quantizing_levels",
				RecalUtils.ARGUMENT_VALUE_COLUMN_NAME, QUANTIZING_LEVELS);
		argumentsTable.addRowID("keep_intermediate_files", true);
		argumentsTable.set("keep_intermediate_files",
				RecalUtils.ARGUMENT_VALUE_COLUMN_NAME, KEEP_INTERMEDIATE_FILES);
		argumentsTable.addRowID("no_plots", true);
		argumentsTable.set("no_plots", RecalUtils.ARGUMENT_VALUE_COLUMN_NAME,
				NO_PLOTS);
		argumentsTable.addRowID("recalibration_report", true);
		// argumentsTable.set("recalibration_report",
		// RecalUtils.ARGUMENT_VALUE_COLUMN_NAME, recalibrationReport == null ?
		// "null" : recalibrationReport.getAbsolutePath());
		argumentsTable.addRowID("binary_tag_name", true);
		argumentsTable.set("binary_tag_name",
				RecalUtils.ARGUMENT_VALUE_COLUMN_NAME,
				BINARY_TAG_NAME == null ? "null" : BINARY_TAG_NAME);
		return argumentsTable;
	}
	
	public String getReferenceSequencePath() {
		return this.reference;
	}

	public List<String> getKnowSite() {
		return knownSites;
	}

	public ArrayList<Path> getInputFileList() {
		return inputList;
	}

	public int getInputType() {
		return inputType;
	}

	public RecalUtils.SOLID_RECAL_MODE getSOLID_RECAL_MODE() {
		return SOLID_RECAL_MODE;
	}

	public void setSOLID_RECAL_MODE(String sOLID_RECAL_MODE) {
		SOLID_RECAL_MODE = RecalUtils.SOLID_RECAL_MODE
				.recalModeFromString(sOLID_RECAL_MODE);
	}

	public RecalUtils.SOLID_NOCALL_STRATEGY getSOLID_NOCALL_STRATEGY() {
		return SOLID_NOCALL_STRATEGY;
	}

	public void setSOLID_NOCALL_STRATEGY(String sOLID_NOCALL_STRATEGY) {
		SOLID_NOCALL_STRATEGY = RecalUtils.SOLID_NOCALL_STRATEGY
				.nocallStrategyFromString(sOLID_NOCALL_STRATEGY);
	}

	public int getReducerNumber() {
		return reducerN;
	}

	public void setReducerNum(int reducerN) {
		this.reducerN = reducerN;
	}
	
	public String getInputString() {
		return input;
	}

	public Path getInput() {

		return new Path(input);
	}

	public Path getTempOutput() {
		if (this.output.endsWith("/"))
			this.tempPath = this.output + "temp";
		else
			this.tempPath = this.output + "/temp";
		return new Path(tempPath);
	}

	public String getOutputPath() {
		return this.output;
	}

	public int getWinSize() {
		return winSize;
	}

	public String getTYPE() {
		return TYPE;
	}

	public boolean isCachedRef() {
		return isCachedRef;
	}

	public boolean isMultiSample() {
		return multiSample;
	}
	
	private void traversalInputPath(Path path) {
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

}
