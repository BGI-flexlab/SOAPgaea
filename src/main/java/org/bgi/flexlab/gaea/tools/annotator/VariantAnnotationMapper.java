package org.bgi.flexlab.gaea.tools.annotator;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderVersion;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bgi.flexlab.gaea.data.structure.header.GaeaSingleVCFHeader;
import org.bgi.flexlab.gaea.tools.annotator.effect.SnpEffectPredictor;

public class VariantAnnotationMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	
	private VCFHeader vcfHeader = null;
	private VCFHeaderVersion vcfVersion = null;
	private SnpEffectPredictor effectPredictor = null;
	private Text resultValue = new Text();
	
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		
		Path inputPath = new Path(conf.get("inputFilePath"));
		
		GaeaSingleVCFHeader singleVcfHeader = new GaeaSingleVCFHeader();
		singleVcfHeader.readHeaderFrom(inputPath, inputPath.getFileSystem(conf));
		vcfHeader = singleVcfHeader.getHeader();
		vcfVersion = singleVcfHeader.getVCFVersion(vcfHeader);
		
//		SnpEffPredictorFactory spf = new SnpEffPredictorFactoryRefSeq();
//		effectPredictor = spf.create();
    	
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		
		String vcfLine = value.toString();
		if (vcfLine.startsWith("#")) {
			return;
		}
		VCFCodec vcfCodec = new VCFCodec();
		vcfCodec.setVCFHeader(vcfHeader, vcfVersion);
		VariantContext variantContext = vcfCodec.decode(vcfLine);
		StringBuilder varTest = new StringBuilder();
		varTest.append(variantContext.getContig());
		varTest.append('-');
		varTest.append(variantContext.getStart());
//		System.out.println(variantContext.getContig() + "-" + variantContext.getStart()+'-' + variantContext.getEnd());
//		variantContext.getAlleles();
//		effectPredictor.variantEffect(variant);
		resultValue.set(varTest.toString());
		context.write(NullWritable.get(), resultValue);
		
	}
}
