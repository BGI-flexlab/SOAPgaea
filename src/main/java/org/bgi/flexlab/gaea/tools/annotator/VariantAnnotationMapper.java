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
import org.bgi.flexlab.gaea.data.structure.reference.ReferenceShare;
import org.bgi.flexlab.gaea.tools.annotator.config.Config;
import org.bgi.flexlab.gaea.tools.annotator.db.DBAnnotator;
import org.bgi.flexlab.gaea.tools.annotator.effect.VcfAnnotationContext;
import org.bgi.flexlab.gaea.tools.annotator.effect.VcfAnnotator;

public class VariantAnnotationMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

	private Text resultValue = new Text();
	private VCFCodec vcfCodec = new VCFCodec();
	private VcfAnnotator vcfAnnotator;
	private DBAnnotator dbAnnotator;
	private Configuration conf;
	long mapTime = 0; 
	long mapCount = 0;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
//		long setupStart = System.currentTimeMillis();
		conf = context.getConfiguration();
		
//		long start = System.currentTimeMillis();
		ReferenceShare genomeShare = new ReferenceShare();
		genomeShare.loadChromosomeList();
//		System.err.println("genomeShare耗时：" + (System.currentTimeMillis()-start)+"毫秒");
		
		Config userConfig = new Config(conf, genomeShare);
		userConfig.setVerbose(conf.getBoolean("verbose", false));
		userConfig.setDebug(conf.getBoolean("debug", false));
		
//		start = System.currentTimeMillis();
		AnnotatorBuild annoBuild = new AnnotatorBuild(userConfig);
		userConfig.setSnpEffectPredictor(annoBuild.createSnpEffPredictor());
		annoBuild.buildForest();
//		System.err.println("build SnpEffectPredictor耗时：" + (System.currentTimeMillis()-start)+"毫秒");
		
		Path inputPath = new Path(conf.get("inputFilePath"));
		
//		start = System.currentTimeMillis();
		SingleVCFHeader singleVcfHeader = new SingleVCFHeader();
		singleVcfHeader.readHeaderFrom(inputPath, inputPath.getFileSystem(conf));
		VCFHeader vcfHeader = singleVcfHeader.getHeader();
		VCFHeaderVersion vcfVersion = singleVcfHeader.getVCFVersion(vcfHeader);
		vcfCodec.setVCFHeader(vcfHeader, vcfVersion);
//		System.err.println("getVCFHeader耗时：" + (System.currentTimeMillis()-start)+"毫秒");
		
		vcfAnnotator = new VcfAnnotator(userConfig);
		
//		start = System.currentTimeMillis();
		//用于从数据库中查找信息
		dbAnnotator = new DBAnnotator(userConfig);
		try {
			dbAnnotator.connection();
		} catch (InstantiationException | IllegalAccessException
				| ClassNotFoundException e) {
			e.printStackTrace();
		}
//		System.err.println("dbAnnotator.connection耗时：" + (System.currentTimeMillis()-start)+"毫秒");
		
		// 注释结果header信息
		resultValue.set(userConfig.getHeader());
		context.write(NullWritable.get(), resultValue);
//		System.err.println("mapper.setup耗时：" + (System.currentTimeMillis()-setupStart)+"毫秒");
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String vcfLine = value.toString();
		if (vcfLine.startsWith("#")) return;
		
		VariantContext variantContext = vcfCodec.decode(vcfLine);
		VcfAnnotationContext vcfAnnoContext = new VcfAnnotationContext(variantContext);
		if(!vcfAnnotator.annotate(vcfAnnoContext)){
			return;
		}
//		System.err.println("vcfAnnotator耗时：" + (System.currentTimeMillis()-start)+"毫秒");
		
		long start = System.currentTimeMillis();
		dbAnnotator.annotate(vcfAnnoContext);
		mapTime += System.currentTimeMillis()-start;
		mapCount++;
//		System.err.println("dbAnnotator耗时：" + (System.currentTimeMillis()-start)+"毫秒");
		
		List<String> annoLines = vcfAnnotator.convertAnnotationStrings(vcfAnnoContext);
		for (String annoLine : annoLines) {
			resultValue.set(annoLine);
			context.write(NullWritable.get(), resultValue);
		}
		
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		dbAnnotator.disconnection();
	}
}
