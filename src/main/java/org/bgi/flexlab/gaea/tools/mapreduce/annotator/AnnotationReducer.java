/*******************************************************************************
 * Copyright (c) 2017, BGI-Shenzhen
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 *******************************************************************************/
package org.bgi.flexlab.gaea.tools.mapreduce.annotator;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFEncoder;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderVersion;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bgi.flexlab.gaea.data.structure.header.SingleVCFHeader;
import org.bgi.flexlab.gaea.data.structure.reference.ReferenceShare;
import org.bgi.flexlab.gaea.tools.annotator.AnnotationEngine;
import org.bgi.flexlab.gaea.tools.annotator.SampleAnnotationContext;
import org.bgi.flexlab.gaea.tools.annotator.VcfAnnoContext;
import org.bgi.flexlab.gaea.tools.annotator.config.Config;
import org.bgi.flexlab.gaea.tools.annotator.db.DBAnnotator;
import org.bgi.flexlab.gaea.util.ChromosomeUtils;

import java.io.IOException;
import java.util.*;

public class AnnotationReducer extends Reducer<Text, VcfLineWritable, Text, Text> {

	private Text resultKey = new Text();
	private Text resultValue = new Text();
	private AnnotatorOptions options;
	private HashMap<String, VCFCodec> vcfCodecs;
	private HashMap<String, VCFHeader> vcfHeaders;
	private AnnotationEngine annoEngine;
	private DBAnnotator dbAnnotator;
	Config userConfig;
	long mapTime = 0;
	long mapCount = 0;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		options = new AnnotatorOptions();
		Configuration conf = context.getConfiguration();
		options.getOptionsFromHadoopConf(conf);

		long setupStart = System.currentTimeMillis();
		long start = System.currentTimeMillis();
		ReferenceShare genomeShare = new ReferenceShare();
		genomeShare.loadChromosomeList(options.getReferenceSequencePath());
		if(options.isDebug())
			System.err.println("genomeShare耗时：" + (System.currentTimeMillis()-start)+"毫秒");

		userConfig = new Config(conf, genomeShare);

		start = System.currentTimeMillis();
		AnnotatorBuild annoBuild = new AnnotatorBuild(userConfig);
		userConfig.setSnpEffectPredictor(annoBuild.createSnpEffPredictor());
		annoBuild.buildForest();
		if(options.isDebug())
			System.err.println("build SnpEffectPredictor耗时：" + (System.currentTimeMillis()-start)+"毫秒");
		vcfCodecs = new HashMap<>();
		vcfHeaders = new HashMap<>();
		Path inputPath = new Path(options.getInputFilePath());
		FileSystem fs = inputPath.getFileSystem(conf);
		FileStatus[] files = fs.listStatus(inputPath);

		for(FileStatus file : files) {
			if (file.isFile()) {
				SingleVCFHeader singleVcfHeader = new SingleVCFHeader();
				singleVcfHeader.readHeaderFrom(file.getPath(), fs);
				VCFHeader vcfHeader = singleVcfHeader.getHeader();
				VCFHeaderVersion vcfVersion = SingleVCFHeader.getVCFHeaderVersion(vcfHeader);
				VCFCodec vcfcodec = new VCFCodec();
				vcfcodec.setVCFHeader(vcfHeader, vcfVersion);
				vcfCodecs.put(file.getPath().getName(), vcfcodec);
				vcfHeaders.put(file.getPath().getName(), vcfHeader);
			}

		}
		if(options.isDebug())
			System.err.println("getVCFHeader耗时：" + (System.currentTimeMillis()-start)+"毫秒");

		annoEngine = new AnnotationEngine(userConfig);

		start = System.currentTimeMillis();
		//用于从数据库中查找信息
		dbAnnotator = new DBAnnotator(userConfig);
		try {
			dbAnnotator.connection();
		} catch (InstantiationException | IllegalAccessException
				| ClassNotFoundException e) {
			e.printStackTrace();
		}
		System.err.println("dbAnnotator.connection耗时：" + (System.currentTimeMillis()-start)+"毫秒");

	}

	@Override
	protected void reduce(Text key, Iterable<VcfLineWritable> values, Context context)
			throws IOException, InterruptedException {
		long start = System.currentTimeMillis();
		Iterator<VcfLineWritable> iter =  values.iterator();
		Map<String, VcfAnnoContext> posVariantInfo = new HashMap<>();
		Map<Integer, String> posToPosKey = new HashMap<>();
		List<Integer> positions = new ArrayList<>();

		while(iter.hasNext()) {
			VcfLineWritable vcfInput =  iter.next();
			String fileName = vcfInput.getFileName();
			String vcfLine = vcfInput.getVCFLine();
			VariantContext variantContext =  vcfCodecs.get(fileName).decode(vcfLine);
//			String refStr = variantContext.getReference().getBaseString();
			int pos = variantContext.getStart();
			int end = variantContext.getEnd();


			if(!positions.contains(pos))
				positions.add(pos);
			String posKey = pos + "-" + Integer.toString(end);
			posToPosKey.put(pos, posKey);
			if(posVariantInfo.containsKey(posKey)){
				posVariantInfo.get(posKey).add(variantContext, fileName);
			}else {
				VcfAnnoContext vcfAnnoContext = new VcfAnnoContext(variantContext, fileName);
				posVariantInfo.put(posKey, vcfAnnoContext);
			}
		}

		Collections.sort(positions);

		for(VcfAnnoContext vcfAnnoContext: posVariantInfo.values()){
			String chr = ChromosomeUtils.getNoChrName(vcfAnnoContext.getContig());
			String posPrefix = chr+"-"+vcfAnnoContext.getStart()/1000;

			if(!posPrefix.equals(key.toString()))
				continue;

			// 标记附近有其他变异的点
			int index = positions.indexOf(vcfAnnoContext.getStart());
			if(index > 0 && vcfAnnoContext.getStart() - positions.get(index-1) <= 5){
				String posKey = posToPosKey.get(positions.get(index-1));
				VcfAnnoContext vcfAnnoContextNear = posVariantInfo.get(posKey);
				for(SampleAnnotationContext sac: vcfAnnoContext.getSampleAnnoContexts().values()){
					String sampleName = sac.getSampleName();
					if(vcfAnnoContextNear.hasSample(sampleName)){
						sac.setHasNearVar();
						vcfAnnoContextNear.getSampleAnnoContexts().get(sampleName).setHasNearVar();
					}
				}
			}

			if(options.isUseDatabaseCache() && !dbAnnotator.annotate(vcfAnnoContext, "ANNO")){
				if (!annoEngine.annotate(vcfAnnoContext)) {
					continue;
				}
				dbAnnotator.annotate(vcfAnnoContext);
				if(options.isDatabaseCache())
					dbAnnotator.insert(vcfAnnoContext, "ANNO");
			}

			if(options.getOutputFormat() == AnnotatorOptions.OutputFormat.VCF){
				Map<String, List<VariantContext>> annos = vcfAnnoContext.toAnnotationVariantContexts(userConfig.getFields());
				for(String filename: annos.keySet()){
					resultKey.set(filename);
					VCFHeader vcfHeader = vcfHeaders.get(filename);
					List<VariantContext> variantContexts = annos.get(filename);
					VCFEncoder vcfEncoder = new VCFEncoder(vcfHeader, true, true);
					for(VariantContext vc: variantContexts){
						String vcfLine = vcfEncoder.encode(vc);
						resultValue.set(vcfLine);
						context.write(resultKey, resultValue);
					}
				}
			}else {
				List<String> annoLines = vcfAnnoContext.toAnnotationStrings(userConfig.getFields());
				for(String sampleName: vcfAnnoContext.getSampleAnnoContexts().keySet())
				{
					resultKey.set(sampleName);
					for(String annoLine: annoLines){
						resultValue.set(annoLine);
						context.write(resultKey, resultValue);
					}
				}
			}
		}

		if(options.isDebug()) {
			System.err.println("step3:" + (System.currentTimeMillis() - start) + "ms");
			mapTime += System.currentTimeMillis() - start;
			mapCount++;
		}
	}

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		dbAnnotator.disconnection();
		if(options.isDebug())
			System.err.println("dbAnnotator平均耗时(mapTime/mapCount)：" +mapTime+"/"+mapCount+" = ? 毫秒");
	}
}
