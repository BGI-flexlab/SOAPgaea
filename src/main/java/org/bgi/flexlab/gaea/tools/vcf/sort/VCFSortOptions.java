package org.bgi.flexlab.gaea.tools.vcf.sort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.bgi.flexlab.gaea.data.mapreduce.input.vcf.VCFRecordReader;
import org.bgi.flexlab.gaea.data.mapreduce.options.HadoopOptions;
import org.bgi.flexlab.gaea.data.structure.header.GaeaVCFHeader;
import org.bgi.flexlab.gaea.options.GaeaOptions;
import org.bgi.flexlab.gaea.util.HdfsFileManager;
import org.seqdoop.hadoop_bam.KeyIgnoringVCFOutputFormat;
import org.seqdoop.hadoop_bam.VCFOutputFormat;

public class VCFSortOptions extends GaeaOptions implements HadoopOptions{
	private final static String SOFTWARE_NAME = "VCFSort";
	private final static String SOFTWARE_VERSION = "1.0";
	
	public VCFSortOptions() {
		addOption("i", "input", true, "input VCF or VCFs(separated by comma), if contains multiple samples, should incorporate with \"m\" ", true);
		addOption("r", "chromosomeFile", true, "chromosome order reference, to indicate how the VCF is sorted", true);
		addOption("o", "output", true, "output directory to restore result", true);
		addOption("m", "multiSample", false, "multiple sample sort", false);
		addOption("h", "help", false, "help information");
		addOption("n", "reducerNumber", true, "number of reducer.(default:30)");
	}

	private String input;
	
	private String tempPath;
		
	private String workPath;
	
	private String output;
	
	private String chrFile;
	
	private int reducerN;
	
	private ArrayList<Path> inputList = new ArrayList<>();
	
	private boolean multiSample;
	
	private Map<Integer, String> multiOutputs;

	
	@Override
	public void setHadoopConf(String[] args, Configuration conf) {
		try {
			String[] otherArgs = new GenericOptionsParser(args).getRemainingArgs();
			conf.setStrings("args", otherArgs);
			conf.set(VCFOutputFormat.OUTPUT_VCF_FORMAT_PROPERTY, "VCF");
			conf.set(GaeaVCFHeader.VCF_HEADER_PROPERTY, setOutputURI("vcfHeader.obj"));
			conf.set(VCFRecordReader.CHR_ORDER_PROPERTY, setOutputURI("chrOrder.obj"));
			conf.setBoolean(KeyIgnoringVCFOutputFormat.WRITE_HEADER_PROPERTY, false);
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	private String setOutputURI(String outputPath){
		StringBuilder uri = new StringBuilder();
		uri.append(output);
		uri.append(System.getProperty("file.separator"));
		uri.append(outputPath);
		return uri.toString();
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
		setWorkPath();
		
		chrFile = getOptionValue("r", null);
		
		reducerN = getOptionIntValue("n", 30);
		
		multiSample = getOptionBooleanValue("m", false);
		
	}
	
	private void traversalInputPath(Path path) {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		FileSystem fs = HdfsFileManager.getFileSystem(path, conf);
		try {
			if (!fs.exists(path)) {
				System.err
						.println("Input File Path is not exist! Please check -i var.");
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
	
	public void setWorkPath() {
		if (this.output.endsWith("/"))
			this.workPath = this.output + "workplace";
		else
			this.workPath = this.output + "/workplace";
	}
	
	public String getWorkPath() {
		return workPath;
	}
	
	public void setMultiOutputs(Map<Integer, String> multiOutputs) {
		this.multiOutputs = multiOutputs;
	}
	
	public Map<Integer, String> getMultiOutputs() {
		return multiOutputs;
	}
	
	public ArrayList<Path> getInputFileList() {
		return inputList;
	}
	
	public String getChrOrderFile() {
		return chrFile;
	}
	
	public int getReducerNumber() {
		return reducerN;
	}
	
	public void setReducerNum(int reducerN) {
		this.reducerN = reducerN;
	}
	
	public String getInput() {
		return input;
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

	public boolean isMultiSample() {
		return multiSample;
	}
}
