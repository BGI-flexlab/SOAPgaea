package org.bgi.flexlab.gaea.tools.mapreduce.recalibrator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.bgi.flexlab.gaea.GaeaRecalibrator.combinar.CombinarTable;
import org.bgi.flexlab.gaea.GaeaRefernce.CacheReference;
import org.bgi.flexlab.gaea.data.mapreduce.input.header.SamFileHeader;
import org.bgi.flexlab.gaea.data.mapreduce.writable.WindowsBasedWritable;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.BioJob;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.ToolsRunner;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.WindowsBasedMapper;
import org.seqdoop.hadoop_bam.SAMFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import htsjdk.samtools.SAMFileHeader;

public class BaseRecalibration extends ToolsRunner{
	
	private static BaseRecalibrationOptions options;
	private static SAMFileHeader mFileHeader;
	
	public BaseRecalibration() {
		this.toolsDescription = "Gaea base recalibration\n"
				+ "this a simple description";
	}

	@Override
	public int run(String[] args) throws Exception {
		options.parse(args);
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
	
	private int runBR(String[] args) throws Exception {
		options.COVARIATES = CovUtils.getCovs(options);
		
		BioJob job = BioJob.getInstance();
		Configuration conf = job.getConfiguration();
		options.setHadoopConf(args, conf);
		
		mFileHeader = SamFileHeader.traversal(options.getInput(), options.getInput().getFileSystem(conf), conf);
		
		if(options.getKnowSite()!=null) {
			for(String site:options.getKnowSite()) {
				VCFIndexCreator creator=new VCFIndexCreator();
				creator.initialize(site, creator.defaultBinSize());
				Index idx=creator.finalizeIndex();
			}
		}
		
		if(options.isCachedRef()){
			CacheReference.disCacheRef(options.getReferenceSequencePath(), conf);
		}
		
		job.setJobName("GaeaBaseRecalibration");
		job.setJarByClass(BaseRecalibration.class);
		job.setWindowsBasicMapperClass(WindowsBasedMapper.class, options.getWinSize());
		job.setReducerClass(BaseRecalibrationReducer.class);
		
		job.setAnySamInputFormat(getformat(options.getInputType()));
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(options.getReducerNumber());
		job.setOutputKeyValue(WindowsBasedWritable.class, SAMRecordWritable.class,
				NullWritable.class, Text.class);
		
		FileInputFormat.setInputPaths(job, 
				options.getInputFileList().toArray(new Path[options.getInputFileList().size()]));
		FileOutputFormat.setOutputPath(job, options.getTempOutput());
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	private void runCM() {
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
	}
}
