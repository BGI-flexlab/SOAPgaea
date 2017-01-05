package org.bgi.flexlab.gaea.tools.mapreduce.realigner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.bgi.flexlab.gaea.data.mapreduce.options.HadoopOptions;
import org.bgi.flexlab.gaea.data.mapreduce.util.HdfsFileManager;
import org.bgi.flexlab.gaea.data.options.GaeaOptions;
import org.bgi.flexlab.gaea.tools.recalibrator.RecalibratorUtil;
import org.bgi.flexlab.gaea.tools.recalibrator.RecalibratorUtil.SolidNocallStrategy;
import org.bgi.flexlab.gaea.tools.recalibrator.RecalibratorUtil.SolidRecallMode;
import org.bgi.flexlab.gaea.tools.recalibrator.report.RecalibratorReportTable;
import org.bgi.flexlab.gaea.util.QualityUtils;
import org.seqdoop.hadoop_bam.SAMFormat;

public class RecalibratorOptions extends GaeaOptions implements HadoopOptions {
	private final static String SOFTWARE_NAME = "BaseRecalibration";
	private final static String SOFTWARE_VERSION = "1.0";

	public RecalibratorOptions() {
		addOption("a", "defaultPlatform", true, "If a read has no platform then default to the provided String."
				+ " Valid options are illumina, 454, and solid.");
		addOption("A", "covariates", true,
				"One or more(separated by comma) covariates to be used in the recalibration.");
		addOption("b", "bintag", true, "the binary tag covariate name if using it");
		addOption("C", "CachedRef", false, "cache reference");
		addOption("D", "ddq", true, "default quality for the base deletions covariate.(default:45)");
		addOption("E", "mdq", true, "default quality for the base mismatches covariate.(default:-1)");
		addOption("F", "idq", true, "default quality for the base insertions covariate.（default:45)");
		addOption("f", "forcePlatform", true, "If provided, the platform of EVERY read will be forced to be the "
				+ "provided String. Valid options are illumina, 454, and solid.");
		addOption("g", "mcs", true, "size of the k-mer context to be used for base mismatches.(default:2)");
		addOption("G", "ics", true,
				"size of the k-mer context to be used for base insertions and deletions.(default:3)");
		addOption("i", "input", true, "input bam or bams or sam(separated by comma)", true);
		addOption("k", "knownSites", true, "know  variant site,for example dbsnp!");
		addOption("K", "ls", false, "If specified, just list the available covariates and exit.");
		addOption("M", "MultiSample", false, "multiple sample list");
		addOption("n", "reducerNumber", true, "number of reducer.(default:30)");
		addOption("N", "noStandard", false,
				"If specified, do not use the standard set of covariates, but rather just the "
						+ "ones listed using the -cov argument.");
		addOption("o", "output", true, "output path", true);
		addOption("p", "muq", true, "minimum quality for the bases to be preserved.");
		addOption("P", "sMode", true,
				"How should we recalibrate solid bases in which the reference was inserted? Options = DO_NOTHING, SET_Q_ZERO, "
						+ "SET_Q_ZERO_BASE_N, or REMOVE_REF_BIAS.");
		addOption("Q", "ql", true, "number of distinct quality scores in the quantized output.(default:16)");
		addOption("r", "reference", true, "reference", true);
		addOption("s", "samformat", false, "input file is sam format");
		addOption("S", "solid_nocall_strategy", true,
				"Defines the behavior of the recalibrator when it encounters no calls in the color space. "
						+ "Options = THROW_EXCEPTION, LEAVE_READ_UNRECALIBRATED, or PURGE_READ.");
		addOption("T", "lqt", true,
				"minimum quality for the bases in the tail of the reads to be considered.(default:2)");
		addOption("w", "winSize", true, "window size.(default:10000)");
		FormatHelpInfo(SOFTWARE_NAME,SOFTWARE_VERSION);
	}

	public boolean LIST_ONLY = false;

	public String[] COVARIATES = null;

	public boolean DO_NOT_USE_STANDARD_COVARIATES = false;

	public SolidRecallMode SOLID_RECAL_MODE = SolidRecallMode.SET_Q_ZERO;

	public SolidNocallStrategy SOLID_NOCALL_STRATEGY = SolidNocallStrategy.THROW_EXCEPTION;

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

	private int reducerNumber;

	private boolean isCachedRef;

	private boolean multiSample;

	private SAMFormat format = SAMFormat.BAM;

	@Override
	public void setHadoopConf(String[] args, Configuration conf) {
		conf.setStrings("args", args);
	}

	@Override
	public void getOptionsFromHadoopConf(Configuration conf) {
		String[] args = conf.getStrings("args");
		this.parse(args);
	}

	public RecalibratorOptions parse(RecalibratorReportTable argsTable) {
		RecalibratorOptions option = new RecalibratorOptions();

		for (int i = 0; i < argsTable.getRowNumber(); i++) {
			final String argument = argsTable.get(i, "Argument").toString();
			Object value = argsTable.get(i, RecalibratorUtil.ARGUMENT_VALUE_COLUMN_NAME);
			if (value.equals("null"))
				value = null;

			if (argument.equals("covariate") && value != null)
				option.COVARIATES = value.toString().split(",");

			else if (argument.equals("standard_covs"))
				option.DO_NOT_USE_STANDARD_COVARIATES = Boolean.parseBoolean((String) value);

			else if (argument.equals("solid_recal_mode"))
				option.SOLID_RECAL_MODE = RecalibratorUtil.SolidRecallMode.recalModeFromString((String) value);

			else if (argument.equals("solid_nocall_strategy"))
				option.SOLID_NOCALL_STRATEGY = RecalibratorUtil.SolidNocallStrategy
						.nocallStrategyFromString((String) value);

			else if (argument.equals("mismatches_context_size"))
				option.MISMATCHES_CONTEXT_SIZE = Integer.parseInt((String) value);

			else if (argument.equals("indels_context_size"))
				option.INDELS_CONTEXT_SIZE = Integer.parseInt((String) value);

			else if (argument.equals("mismatches_default_quality"))
				option.MISMATCHES_DEFAULT_QUALITY = Byte.parseByte((String) value);

			else if (argument.equals("insertions_default_quality"))
				option.INSERTIONS_DEFAULT_QUALITY = Byte.parseByte((String) value);

			else if (argument.equals("deletions_default_quality"))
				option.DELETIONS_DEFAULT_QUALITY = Byte.parseByte((String) value);

			else if (argument.equals("low_quality_tail"))
				option.LOW_QUAL_TAIL = Byte.parseByte((String) value);

			else if (argument.equals("default_platform"))
				option.DEFAULT_PLATFORM = (String) value;

			else if (argument.equals("force_platform"))
				option.FORCE_PLATFORM = (String) value;

			else if (argument.equals("quantizing_levels"))
				option.QUANTIZING_LEVELS = Integer.parseInt((String) value);

			else if (argument.equals("keep_intermediate_files"))
				option.KEEP_INTERMEDIATE_FILES = Boolean.parseBoolean((String) value);

			else if (argument.equals("no_plots"))
				option.NO_PLOTS = Boolean.parseBoolean((String) value);

			else if (argument.equals("binary_tag_name"))
				option.BINARY_TAG_NAME = (value == null) ? null : (String) value;
		}

		return option;
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

		if (getOptionBooleanValue("s", false))
			format = SAMFormat.SAM;

		reference = getOptionValue("r", null);

		if (knownSites == null)
			knownSites = new ArrayList<String>();
		this.knownSites.add(getOptionValue("k", null));

		BINARY_TAG_NAME = getOptionValue("b", null);

		DEFAULT_PLATFORM = getOptionValue("a", null);

		FORCE_PLATFORM = getOptionValue("f", null);

		COVARIATES = getOptionValue("A", null) == null ? null : getOptionValue("c", null).split(",");

		MISMATCHES_CONTEXT_SIZE = getOptionIntValue("g", 2);

		INDELS_CONTEXT_SIZE = getOptionIntValue("G", 3);

		MISMATCHES_DEFAULT_QUALITY = (byte) getOptionIntValue("E", -1);

		INSERTIONS_DEFAULT_QUALITY = (byte) getOptionIntValue("F", 45);

		DELETIONS_DEFAULT_QUALITY = (byte) getOptionIntValue("D", 45);

		LOW_QUAL_TAIL = (byte) getOptionIntValue("T", 2);

		PRESERVE_QSCORES_LESS_THAN = getOptionIntValue("p", QualityUtils.MINIMUM_USABLE_QUALITY_SCORE);

		QUANTIZING_LEVELS = getOptionIntValue("Q", 16);

		reducerNumber = getOptionIntValue("n", 30);

		winSize = getOptionIntValue("w", 1000000);

		LIST_ONLY = getOptionBooleanValue("K", false);

		DO_NOT_USE_STANDARD_COVARIATES = getOptionBooleanValue("N", false);

		setSOLID_RECAL_MODE(getOptionValue("P", null));

		setSOLID_NOCALL_STRATEGY(getOptionValue("S", null));

		isCachedRef = getOptionBooleanValue("C", false);

		multiSample = getOptionBooleanValue("M", false);
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

	public SAMFormat getInputType() {
		return format;
	}

	public SolidRecallMode getSOLID_RECAL_MODE() {
		return SOLID_RECAL_MODE;
	}

	@SuppressWarnings("static-access")
	public void setSOLID_RECAL_MODE(String optionValue) {
		if (optionValue == null)
			SOLID_RECAL_MODE = SOLID_RECAL_MODE.SET_Q_ZERO;
		else
			SOLID_RECAL_MODE = SOLID_RECAL_MODE.recalModeFromString(optionValue);
	}

	public SolidNocallStrategy getSOLID_NOCALL_STRATEGY() {
		return SOLID_NOCALL_STRATEGY;
	}

	public void setSOLID_NOCALL_STRATEGY(String optionValue) {
		if (optionValue == null)
			SOLID_NOCALL_STRATEGY = SolidNocallStrategy.THROW_EXCEPTION;
		else
			SOLID_NOCALL_STRATEGY = SolidNocallStrategy.nocallStrategyFromString(optionValue);
	}

	public int getReducerNumber() {
		return reducerNumber;
	}

	public void setReducerNum(int reducerN) {
		this.reducerNumber = reducerN;
	}

	public String getInputString() {
		return input;
	}

	public Path getInput() {

		return new Path(input);
	}

	public String getTempOutput() {
		if (this.output.endsWith("/"))
			this.tempPath = this.output + "temp";
		else
			this.tempPath = this.output + "/temp";
		return this.tempPath;
	}

	public String getOutputPath() {
		return this.output;
	}

	public int getWindowsSize() {
		return winSize;
	}

	public boolean isCachedRef() {
		return isCachedRef;
	}

	public boolean isMultiSample() {
		return multiSample;
	}

	private void traversalInputPath(Path path) {
		Configuration conf = new Configuration();
		FileSystem fs = HdfsFileManager.getFileSystem(path, conf);
		try {
			if (!fs.exists(path)) {
				System.err.println("Input File Path is not exist! Please check -I var.");
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
