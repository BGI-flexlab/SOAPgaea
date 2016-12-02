package org.bgi.flexlab.gaea.tools.vcf.sort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.bgi.flexlab.gaea.data.mapreduce.options.HadoopOptions;
import org.bgi.flexlab.gaea.options.GaeaOptions;
import org.bgi.flexlab.gaea.util.HdfsFileManager;
import org.seqdoop.hadoop_bam.KeyIgnoringVCFOutputFormat;
import org.seqdoop.hadoop_bam.VCFOutputFormat;
import org.seqdoop.hadoop_bam.util.VCFHeaderReader;
import org.seqdoop.hadoop_bam.util.WrapSeekable;

import htsjdk.tribble.readers.AsciiLineReader;
import htsjdk.tribble.readers.AsciiLineReaderIterator;
import htsjdk.variant.vcf.VCFHeader;

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
	
	private Map<Long, VCFHeader> headerID = new HashMap<>();
	
	private Map<String, Long> chrOrder = new HashMap<>();
	
	private Map<Long, String> multiOutputs = new HashMap<>();

	
	@Override
	public void setHadoopConf(String[] args, Configuration conf) {
		try {
			String[] otherArgs = new GenericOptionsParser(args).getRemainingArgs();
			conf.setStrings("args", otherArgs);
			conf.set(VCFOutputFormat.OUTPUT_VCF_FORMAT_PROPERTY, "VCF");
			conf.setBoolean(KeyIgnoringVCFOutputFormat.WRITE_HEADER_PROPERTY, false);
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
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
		setHeaderID();
		
		output = getOptionValue("o", null);
		setWorkPath();
		setMultiOutputPath();
		
		chrFile = getOptionValue("r", null);
		try {
			initChrOrder();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new RuntimeException(e);
		}
		
		reducerN = getOptionIntValue("n", 30);
		
		multiSample = getOptionBooleanValue("m", false);
		
	}
	
	private void traversalInputPath(Path path) {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		FileSystem fs = HdfsFileManager.getFileSystem(path, conf);
		long i = 0;
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

	private void setHeaderID() {
		long id = 0;
		VCFHeader oldHeader = null;
		Configuration conf = new Configuration();
		for(Path input : inputList) {
			try {
				final WrapSeekable ins = WrapSeekable.openPath(conf, input);
				VCFHeader header = VCFHeaderReader.readHeaderFrom(ins);
				if(!header.equals(oldHeader))
					headerID.put(++id << 40, header);
				ins.close();
				oldHeader = header;
			} catch(IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	public Map<Long, VCFHeader> getHeaderID() {
		return headerID;
	}
	
	private void setMultiOutputPath() {
		int i = 0;
		for(long id : getHeaderID().keySet()) {
			multiOutputs.put(id, "SortResult" + ++i);
		}
	}
	
	public Map<Long, String> getMultiOutputPath() {
		return multiOutputs;
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
	
	private void initChrOrder() throws IOException {
		Configuration conf = new Configuration();
		chrOrder.clear();
        FSDataInputStream ins = HdfsFileManager.getInputStream(new Path(chrFile), conf);
        AsciiLineReaderIterator it = new AsciiLineReaderIterator(new AsciiLineReader(ins));
        String line;  
        long i = 1;
        while(it.hasNext()){
        	line = it.next();
        	//System.err.println("fai:" + line);
        	String[] cols = line.split("\t");
        	chrOrder.put(cols[0].trim(), i++ << 32 );
        }
        it.close();
	}
	
	public Map<String, Long> getChrOrder() {
		return chrOrder;
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
