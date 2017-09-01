package org.bgi.flexlab.gaea.tools.mapreduce.jointcalling;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.bgi.flexlab.gaea.data.mapreduce.input.vcf.VCFHdfsLoader;
import org.bgi.flexlab.gaea.data.mapreduce.output.vcf.GaeaVCFOutputFormat;
import org.bgi.flexlab.gaea.data.mapreduce.output.vcf.VCFHdfsWriter;
import org.bgi.flexlab.gaea.data.mapreduce.writable.WindowsBasedWritable;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.BioJob;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.ToolsRunner;
import org.bgi.flexlab.gaea.tools.jointcalling.util.GaeaGvcfVariantContextUtils;
import org.seqdoop.hadoop_bam.KeyIgnoringVCFOutputFormat;
import org.seqdoop.hadoop_bam.VCFInputFormat;
import org.seqdoop.hadoop_bam.VariantContextWritable;

import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLine;
import htsjdk.variant.vcf.VCFUtils;

public class JointCalling extends ToolsRunner{
	
	public JointCalling(){
		this.toolsDescription = "joing calling for gvcfs";
	}
	
	private Set<VCFHeader> getVCFHeaders(List<Path> inputs) throws IOException{
		Set<VCFHeader> headerSet = new HashSet<VCFHeader>();
		
		for(Path p : inputs){
			VCFHdfsLoader loader = new VCFHdfsLoader(p.toString());
			headerSet.add(loader.getHeader());
			loader.close();
		}
		
		return headerSet;
	}
	
	private Set<String> getSampleList(Set<VCFHeader> headers){
		Set<String> samples = new TreeSet<String>();
		for(VCFHeader header : headers){
			for ( String sample : header.getGenotypeSamples() ) {
				samples.add(GaeaGvcfVariantContextUtils.mergedSampleName(null, sample, false));
			}
		}
		
		return samples;
	}
	
	private VCFHeader getVCFHeaderFromInput(List<Path> inputs) throws IOException{
		Set<VCFHeader> headers = getVCFHeaders(inputs);
        Set<String> samples = getSampleList(headers);
        Set<VCFHeaderLine> headerLines = VCFUtils.smartMergeHeaders(headers, true);
        VCFHeader vcfHeader = new VCFHeader(headerLines, samples);
        
        headers.clear();
        samples.clear();
        headerLines.clear();
        
        return vcfHeader;
	}

	@Override
	public int run(String[] args) throws Exception {
		BioJob job = BioJob.getInstance();
        Configuration conf = job.getConfiguration();
        
        String[] remainArgs = remainArgs(args, conf);
        JointCallingOptions options = new JointCallingOptions();
        options.parse(remainArgs);
        options.setHadoopConf(remainArgs, conf);
        conf.set(GaeaVCFOutputFormat.OUT_PATH_PROP, options.getVCFHeaderOutput() + "/vcfFileHeader.vcf");
        conf.set(KeyIgnoringVCFOutputFormat.OUTPUT_VCF_FORMAT_PROPERTY, options.getOuptputFormat().toString());
        
        VCFHeader vcfHeader = getVCFHeaderFromInput(options.getInput());
        VCFHdfsWriter vcfHdfsWriter = new VCFHdfsWriter(conf.get(GaeaVCFOutputFormat.OUT_PATH_PROP), false, false, conf);
        vcfHdfsWriter.writeHeader(vcfHeader);
        vcfHdfsWriter.close();
        
        job.setJobName("Gaea joint calling");
        
        job.setJarByClass(JointCalling.class);
        job.setMapperClass(JointCallingMapper.class);
        job.setReducerClass(JointCallingReducer.class);
        job.setNumReduceTasks(options.getReducerNumber());
        job.setOutputKeyValue(WindowsBasedWritable.class,VariantContextWritable.class, NullWritable.class, VariantContextWritable.class);
        
        job.setInputFormatClass(VCFInputFormat.class);
		job.setOutputFormatClass(GaeaVCFOutputFormat.class);
        
        FileInputFormat.setInputPaths(job, options.getInput().toArray(new Path[options.getInput().size()]));
		FileOutputFormat.setOutputPath(job, new Path(options.getOutput()));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
