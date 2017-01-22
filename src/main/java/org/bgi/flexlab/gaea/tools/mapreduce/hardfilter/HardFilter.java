package org.bgi.flexlab.gaea.tools.mapreduce.hardfilter;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.bgi.flexlab.gaea.data.mapreduce.input.vcf.VCFMultipleInputFormat;
import org.bgi.flexlab.gaea.data.mapreduce.util.HdfsFileManager;
import org.bgi.flexlab.gaea.data.structure.header.GaeaVCFHeader;
import org.bgi.flexlab.gaea.data.structure.header.MultipleVCFHeader;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.BioJob;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.ToolsRunner;
import org.bgi.flexlab.gaea.tools.hardfilter.FilterEngine;
import org.bgi.flexlab.gaea.tools.vcf.report.ReportDatum;
import org.seqdoop.hadoop_bam.KeyIgnoringVCFOutputFormat;
import org.seqdoop.hadoop_bam.VariantContextWritable;

import hbparquet.hadoop.util.ContextUtil;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;

public class HardFilter extends ToolsRunner{

    public HardFilter() {
		this.toolsDescription = "Gaea variant hard filter\n"
				+ "The purpose of hard filter is to filter out variants that"
				+ "don't meet the criterion of specific annotation value";
	}
    
    private HardFilterOptions options;
    private MultipleVCFHeader vcfHeaders;

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		BioJob job = BioJob.getInstance();
		Configuration conf = job.getConfiguration();
		
		options = new HardFilterOptions();
		options.parse(args);
		options.setHadoopConf(args, conf);
		
		//merge header
		vcfHeaders = new MultipleVCFHeader();
		vcfHeaders.mergeHeader(new Path(options.getInput()), options.getOutput(), job, false);
				
		job.setJarByClass(HardFilter.class);
		job.setMapperClass(HardFilterMapper.class);
		job.setReducerClass(HardFilterReducer.class);
		job.setOutputKeyValue(IntWritable.class, Text.class, 
				NullWritable.class, VariantContextWritable.class);
		job.setNumReduceTasks(vcfHeaders.getFileNum());
		
		FileInputFormat.addInputPaths(job, options.getInput());
		job.setInputFormatClass(VCFMultipleInputFormat.class);

		job.setOutputFormatClass(HardFilterOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(options.getOutput()));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	
}

final class HardFilterMapper extends Mapper<LongWritable, VariantContextWritable, IntWritable, VariantContextWritable> {	
	
	private FilterEngine fe;
	
	private HardFilterOptions options;
	
	@Override
	public void setup(Context context) {
		fe = new FilterEngine(options.getFilterName(), options.getSnpFilter(), options.getIndelFilter());
	}

	@Override
	public void map(LongWritable key, VariantContextWritable value, Context context) throws IOException, InterruptedException {
		VariantContextWritable vcw = new VariantContextWritable();
		vcw.set(fe.filter(value.get()));
		context.write(new IntWritable((int) key.get()), vcw);
	}
}

final class HardFilterReducer extends Reducer<IntWritable, VariantContextWritable, NullWritable, VariantContextWritable> {
	
	private FilterEngine fe;
	
	private HardFilterOptions options;
	
	private ReportDatum report;
	
	private MultipleVCFHeader mHeader;
	
	public static VCFHeader finalHeader;
	
	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		fe = new FilterEngine(options.getFilterName(), options.getSnpFilter(), options.getIndelFilter());
		mHeader = (MultipleVCFHeader) GaeaVCFHeader.loadVcfHeader(false, conf);
	}
	
	@Override
	public void reduce(IntWritable key, Iterable<VariantContextWritable> values, Context context ) {
		VCFHeader header = mHeader.getVcfHeader(key.get());
		finalHeader = fe.addFilterHeader(header);
		for(VariantContextWritable vcw : values) {
			VariantContext vc = vcw.get();
			ReportDatum datum = new ReportDatum.Builder(vc).isSnp().isIndel().isTransition().build();
			if(report == null)
				report = datum;
			else
				report.combine(datum);
		}
	}
	
	@Override
	public void cleanup(Context context) throws IOException {
		FSDataOutputStream os = HdfsFileManager.getOutputStream(new Path(options.getReportPath()), context.getConfiguration());
		os.write(report.formatReport().getBytes());
		os.close();
	}
}


final class HardFilterOutputFormat<K> extends FileOutputFormat<K, VariantContextWritable>{
	
	private KeyIgnoringVCFOutputFormat<K> baseOF;
	
	private void initBaseOF(Configuration conf) {
		if (baseOF == null)
			baseOF = new KeyIgnoringVCFOutputFormat<K>(conf);
	}

	@Override public RecordWriter<K,VariantContextWritable> getRecordWriter(
			TaskAttemptContext context)
		throws IOException {
		final Configuration conf = ContextUtil.getConfiguration(context);
		initBaseOF(conf);
		baseOF.setHeader(HardFilterReducer.finalHeader);
	
		return baseOF.getRecordWriter(context, getDefaultWorkFile(context, ""));
	}

	// Allow the output directory to exist.
	@Override public void checkOutputSpecs(JobContext job) {}
}