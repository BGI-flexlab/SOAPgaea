package org.bgi.flexlab.gaea.tools.mapreduce.recalibrator;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.bgi.flexlab.gaea.data.mapreduce.input.header.SamHdfsFileHeader;
import org.bgi.flexlab.gaea.data.mapreduce.writable.WindowsBasedWritable;
import org.bgi.flexlab.gaea.data.structure.bam.filter.SamRecordFilter;
import org.bgi.flexlab.gaea.data.structure.vcf.index.Index;
import org.bgi.flexlab.gaea.data.structure.vcf.index.VCFIndexCreator;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.BioJob;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.ToolsRunner;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.WindowsBasedMapper;
import org.bgi.flexlab.gaea.tools.baserecalibration.BaseRecalibrationFilter;
import org.bgi.flexlab.gaea.tools.baserecalibration.RecalibrationUtils;
import org.bgi.flexlab.gaea.tools.baserecalibration.covariates.Covariate;
import org.bgi.flexlab.gaea.tools.mapreduce.recalibrator.basequalityscorerecalibration.BaseRecalibrationReducer;
import org.bgi.flexlab.gaea.tools.mapreduce.recalibrator.combinar.CombinarTable;
import org.bgi.flexlab.gaea.util.Pair;
import org.seqdoop.hadoop_bam.SAMFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import htsjdk.samtools.SAMFileHeader;

public class BaseRecalibration extends ToolsRunner{
	
	private static BaseRecalibrationOptions options;
	private static SAMFileHeader mFileHeader;
	
	
	public BaseRecalibration() {
		this.toolsDescription = "Gaea base quality score recalibration\n"
				+ "it is a data pre-processing step that detects systematic errors made by the sequencer "
				+ "when it estimates the quality score of each base call";
	}
	
	@Override
	public int run(String[] args) throws Exception {
		options = new BaseRecalibrationOptions();
		options.parse(args);
		options.COVARIATES = getCovs(options);
		
		if (options.getTYPE().equals("ALL")) {
			if (runBR(args)==0) {
				Thread.sleep(10000);
				runCM();
			} else {
				System.out.println("BQSR hadoop aplication has error!");
				System.exit(-1);
			}
		} else if(options.getTYPE().equals("BR")) {
			runBR(args);
		} else if(options.getTYPE().equals("CM")) {
			runCM();
		} else {
			System.out.println("unknow type.please check it");
			return -1;
		}
		return 0;
		
	}
	
	public int runBR(String[] args) throws Exception {		
		BioJob job = BioJob.getInstance();
		Configuration conf = job.getConfiguration();
		options.setHadoopConf(args, conf);
		
		mFileHeader = SamHdfsFileHeader.loadHeader(options.getInput(), conf, new Path(options.getOutputPath()));
		SamRecordFilter filter = new BaseRecalibrationFilter();
		conf.set(WindowsBasedMapper.SAM_RECORD_FILTER, filter.getClass().getName());
		createIndex();
	
		job.setJobName("GaeaBaseRecalibration");
		job.setJarByClass(BaseRecalibration.class);
		job.setWindowsBasicMapperClass(WindowsBasedMapper.class, options.getWinSize(), 0);
		job.setReducerClass(BaseRecalibrationReducer.class);
		
		job.setAnySamInputFormat(getformat(options.getInputType()));
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(options.getReducerNumber());
		job.setOutputKeyValue(WindowsBasedWritable.class, SAMRecordWritable.class,
				NullWritable.class, Text.class);
		
		FileInputFormat.setInputPaths(job, 
				options.getInputFileList().toArray(new Path[options.getInputFileList().size()]));
		FileOutputFormat.setOutputPath(job, new Path(options.getTempOutput()));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	/**
	 * Combine sub tables generated from base recalibration, this step runs on local machine. 
	 * @throws IOException 
	 */
	public void runCM() throws IOException {
		CombinarTable combinar=new CombinarTable(mFileHeader, options);
		combinar.initialize();
		combinar.updateDataForTable(options.getTempOutput());
		combinar.onCombianarDone();
	}

	private static SAMFormat getformat(int type) {
		if(type == 0) {
			return SAMFormat.BAM;
		} else if(type == 1) {
			return SAMFormat.SAM;
		}
		throw new RuntimeException("Invalid arguments for input type, only 0(BAM) or 1(SAM)"
				+ "can be accepted");
	}
	
	private void createIndex() throws IOException {
		if(options.getKnowSite()!=null) {
			for(String site:options.getKnowSite()) {
				VCFIndexCreator creator=new VCFIndexCreator();
				creator.initialize(site, creator.defaultBinSize());
				Index idx = creator.finalizeIndex();
			}
		}
	}
	
	private static String[] getCovs(BaseRecalibrationOptions option) {
		Pair<ArrayList<Covariate>, ArrayList<Covariate>> covariates = RecalibrationUtils
				.initializeCovariates(option);
		String[] covs = new String[covariates.getFirst().size()
				+ covariates.getSecond().size()];
		int index = 0;
		for(Covariate cov : covariates.getFirst()){
			covs[index] = cov.getClass().getSimpleName();
			index++;
		}
		for(Covariate cov : covariates.getSecond()){
			covs[index] = cov.getClass().getSimpleName();
			index++;
		}
		return covs;
	}

	public static void main(String[] args) throws Exception {
		BaseRecalibration br = new BaseRecalibration();
		br.run(args);
	}
}
