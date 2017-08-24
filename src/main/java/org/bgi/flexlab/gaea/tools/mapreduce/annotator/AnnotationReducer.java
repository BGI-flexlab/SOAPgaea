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

import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderVersion;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.bgi.flexlab.gaea.data.structure.header.SingleVCFHeader;
import org.bgi.flexlab.gaea.data.structure.reference.ReferenceShare;
import org.bgi.flexlab.gaea.tools.annotator.config.Config;
import org.bgi.flexlab.gaea.tools.annotator.db.DBAnnotator;
import org.bgi.flexlab.gaea.tools.annotator.effect.VcfAnnotationContext;
import org.bgi.flexlab.gaea.tools.annotator.effect.VcfAnnotator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class AnnotationReducer extends Reducer<Text, VcfLineWritable, NullWritable, Text> {

	private Text resultValue = new Text();
	private HashMap<String, VCFCodec> vcfCodecs;
	private List<String> sampleNames;
	private VcfAnnotator vcfAnnotator;
	private DBAnnotator dbAnnotator;
	private Configuration conf;
	private MultipleOutputs<NullWritable, Text> multipleOutputs;
	long mapTime = 0; 
	long mapCount = 0;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		long setupStart = System.currentTimeMillis();
		conf = context.getConfiguration();
		
		long start = System.currentTimeMillis();
		ReferenceShare genomeShare = new ReferenceShare();
		genomeShare.loadChromosomeList();

		System.err.println("genomeShare耗时：" + (System.currentTimeMillis()-start)+"毫秒");
		
		Config userConfig = new Config(conf, genomeShare);
		userConfig.setVerbose(conf.getBoolean("verbose", false));
		userConfig.setDebug(conf.getBoolean("debug", false));
		
		start = System.currentTimeMillis();
		AnnotatorBuild annoBuild = new AnnotatorBuild(userConfig);
		userConfig.setSnpEffectPredictor(annoBuild.createSnpEffPredictor());
		annoBuild.buildForest();
		System.err.println("build SnpEffectPredictor耗时：" + (System.currentTimeMillis()-start)+"毫秒");
		sampleNames = new ArrayList<>();
		vcfCodecs = new HashMap<>();
		Path inputPath = new Path(conf.get("inputFilePath"));
		FileSystem fs = inputPath.getFileSystem(conf);
		FileStatus[] files = fs.listStatus(inputPath);

		for(FileStatus file : files) {
			System.out.println(file.getPath());
			if (file.isFile()) {
				SingleVCFHeader singleVcfHeader = new SingleVCFHeader();
				singleVcfHeader.readHeaderFrom(file.getPath(), fs);
				VCFHeader vcfHeader = singleVcfHeader.getHeader();
				VCFHeaderVersion vcfVersion = singleVcfHeader.getVCFVersion(vcfHeader);
				VCFCodec vcfcodec = new VCFCodec();
				vcfcodec.setVCFHeader(vcfHeader, vcfVersion);
				vcfCodecs.put(file.getPath().getName(), vcfcodec);
				System.out.println("getname: "+file.getPath().getName());

				sampleNames.addAll(vcfHeader.getSampleNamesInOrder());
				System.out.println(sampleNames.toString());
			}

		}


		multipleOutputs = new MultipleOutputs(context);
		System.err.println("getVCFHeader耗时：" + (System.currentTimeMillis()-start)+"毫秒");
		
		vcfAnnotator = new VcfAnnotator(userConfig);
		
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
		
		// 注释结果header信息
		resultValue.set(userConfig.getHeader());
		for(int i = 0; i < sampleNames.size(); i ++) {
			multipleOutputs.write(SampleNameModifier.modify(sampleNames.get(i)), NullWritable.get(), resultValue, sampleNames.get(i) + "/part");
		}
		System.err.println("mapper.setup耗时：" + (System.currentTimeMillis()-setupStart)+"毫秒");
	}

	@Override
	protected void reduce(Text key, Iterable<VcfLineWritable> values, Context context)
			throws IOException, InterruptedException {

		Iterator<VcfLineWritable> iter =  values.iterator();
		List<VcfAnnotationContext> vcfList = new ArrayList<>();
		while(iter.hasNext()) {
			VcfLineWritable vcfInput =  iter.next();
			String fileName = vcfInput.getFileName();
			String vcfLine = vcfInput.getVCFLine();
			System.out.println("reducer: " + key.toString() + " " + vcfLine);

			VariantContext variantContext =  vcfCodecs.get(fileName).decode(vcfLine);
			VcfAnnotationContext vcfAnnoContext = new VcfAnnotationContext(variantContext);
			if (!vcfAnnotator.annotate(vcfAnnoContext)) {
				continue;
			}
			vcfList.add(vcfAnnoContext);
		}

		/*相同key只查询一次*/
		dbAnnotator.annotate(vcfList);
		mapCount++;

		for( int i = 0 ; i < vcfList.size(); i ++) {
			VcfAnnotationContext vcfAnnoContext = vcfList.get(i);
			List<String> annoLines = vcfAnnotator.convertAnnotationStrings(vcfAnnoContext);
			for (int j = 0; j < vcfAnnoContext.getNSamples(); j ++) {
				Genotype genotype = vcfAnnoContext.getGenotype(j);
				if(genotype.isCalled())
				for (String annoLine : annoLines) {
					resultValue.set(annoLine);
					multipleOutputs.write(SampleNameModifier.modify(genotype.getSampleName()), NullWritable.get(),
							resultValue, genotype.getSampleName() + "/part");
				}
			}
		}
	}

		

	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		dbAnnotator.disconnection();
		multipleOutputs.close();
		System.err.println("dbAnnotator平均耗时(mapTime/mapCount)：" +mapTime+"/"+mapCount+" = ? 毫秒");
	}
}
