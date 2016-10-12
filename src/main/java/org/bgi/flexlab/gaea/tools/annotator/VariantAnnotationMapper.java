package org.bgi.flexlab.gaea.tools.annotator;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderVersion;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bgi.flexlab.gaea.data.structure.header.SingleVCFHeader;
import org.bgi.flexlab.gaea.data.structure.reference.GenomeShare;
import org.bgi.flexlab.gaea.tools.annotator.config.Config;
import org.bgi.flexlab.gaea.tools.annotator.db.DBAnno;
import org.bgi.flexlab.gaea.tools.annotator.effect.VcfAnnotationContext;
import org.bgi.flexlab.gaea.tools.annotator.effect.VcfAnnotator;

public class VariantAnnotationMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

	private Text resultValue = new Text();
	private VCFCodec vcfCodec = new VCFCodec();
	private VcfAnnotator vcfAnnotator = null;
	private DBAnno dbAnno = null;
	private Configuration conf;
	
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		conf = context.getConfiguration();

		GenomeShare genomeShare = new GenomeShare();
		if (conf.get("cacheref") != null)
			genomeShare.loadChromosomeList();
		else
			genomeShare.loadChromosomeList(conf.get("reference"));

		Config userConfig = new Config(conf, genomeShare);
		AnnotatorBuild annoBuild = new AnnotatorBuild(userConfig);
		
		userConfig.setSnpEffectPredictor(annoBuild.createSnpEffPredictor());
		annoBuild.buildForest();
		
		Path inputPath = new Path(conf.get("inputFilePath"));
		
		SingleVCFHeader singleVcfHeader = new SingleVCFHeader();
		singleVcfHeader.readHeaderFrom(inputPath, inputPath.getFileSystem(conf));
		VCFHeader vcfHeader = singleVcfHeader.getHeader();
		VCFHeaderVersion vcfVersion = singleVcfHeader.getVCFVersion(vcfHeader);
		vcfCodec.setVCFHeader(vcfHeader, vcfVersion);
		
		vcfAnnotator = new VcfAnnotator(userConfig);
		
		//用于从数据库中查找信息
		dbAnno = new DBAnno(userConfig);
		
		// 注释结果header信息
		resultValue.set(userConfig.getHeader());
		context.write(NullWritable.get(), resultValue);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		
		String vcfLine = value.toString();
		if (vcfLine.startsWith("#")) {
			return;
		}
		
		VariantContext variantContext = vcfCodec.decode(vcfLine);
		VcfAnnotationContext vcfAnnoContext = new VcfAnnotationContext(variantContext);
		
		if(!vcfAnnotator.annotate(vcfAnnoContext)){
			return;
		}
		
		
//		dbAnno.annotate(vcfAnnoContext);
		
		
		if (conf.get("outputType").equals("txt")) {
			List<String> annoLines = vcfAnnotator.convertAnnotationStrings(vcfAnnoContext);
			for (String annoLine : annoLines) {
				resultValue.set(annoLine);
				context.write(NullWritable.get(), resultValue);
			}
		}else {
//			TODO other output format
		}
		
	}
}
