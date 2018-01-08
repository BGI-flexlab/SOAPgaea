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
package org.bgi.flexlab.gaea.tools.mapreduce.jointcallingEval;

import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderVersion;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bgi.flexlab.gaea.data.structure.header.SingleVCFHeader;
import org.bgi.flexlab.gaea.tools.mapreduce.annotator.VcfLineWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class JointcallingEvalReducer extends Reducer<Text, VcfLineWritable, NullWritable, Text> {

	private Text resultValue = new Text();
	private List<String> sampleNames;
	private HashMap<String, VCFCodec> vcfCodecs;
	private Path inputPath;

	//	test, baseline, intersection
	private HashMap<String, int[]> stat;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		JointcallingEcalOptions options = new JointcallingEcalOptions();
		Configuration conf = context.getConfiguration();
		options.getOptionsFromHadoopConf(conf);
		sampleNames = new ArrayList<>();
		vcfCodecs = new HashMap<>();
		stat = new HashMap<>();
		stat.put("Total", new int[3]);

		inputPath = new Path(options.getInputFilePath());
		FileSystem fs = inputPath.getFileSystem(conf);
		SingleVCFHeader singleVcfHeader = new SingleVCFHeader();
		singleVcfHeader.readHeaderFrom(inputPath, fs);
		VCFHeader vcfHeader = singleVcfHeader.getHeader();
		VCFHeaderVersion vcfVersion = singleVcfHeader.getVCFVersion(vcfHeader);
		VCFCodec vcfcodec = new VCFCodec();
		vcfcodec.setVCFHeader(vcfHeader, vcfVersion);
		vcfCodecs.put(inputPath.getName(), vcfcodec);
		sampleNames.addAll(vcfHeader.getSampleNamesInOrder());

		Path baselinePath = new Path(options.getBaselineFile());
		fs = baselinePath.getFileSystem(conf);
		singleVcfHeader = new SingleVCFHeader();
		singleVcfHeader.readHeaderFrom(baselinePath, fs);
		vcfHeader = singleVcfHeader.getHeader();
		vcfVersion = singleVcfHeader.getVCFVersion(vcfHeader);
		vcfcodec = new VCFCodec();
		vcfcodec.setVCFHeader(vcfHeader, vcfVersion);
		vcfCodecs.put(baselinePath.getName(), vcfcodec);
	}

	@Override
	protected void reduce(Text key, Iterable<VcfLineWritable> values, Context context)
			throws IOException, InterruptedException {
		VariantContext testVariantContext = null;
		VariantContext baselineVariantContext = null;
		Iterator<VcfLineWritable> iter =  values.iterator();
		int count = 0;

		while(iter.hasNext()) {
			VcfLineWritable vcfInput = iter.next();
			String tag = vcfInput.getFileName();
			if (tag.equals(inputPath.getName())) {
				testVariantContext = vcfCodecs.get(tag).decode(vcfInput.getVCFLine());
//				System.out.println(testVariantContext.getReference().toString() + ":" + testVariantContext.getAlleles().toString()+ ":" + testVariantContext.getNAlleles());
				stat.get("Total")[0] += 1;
			} else {
				baselineVariantContext = vcfCodecs.get(tag).decode(vcfInput.getVCFLine());
				stat.get("Total")[1] += 1;
			}
			count++;
		}
		if(count != 2)
			System.out.println("Count is: " + count );

		if(testVariantContext != null && baselineVariantContext != null && testVariantContext.hasSameAllelesAs(baselineVariantContext)){
			stat.get("Total")[2] ++;
		}

		for (String sampleName : sampleNames){
			if(stat.containsKey(sampleName)){
				if(testVariantContext != null && baselineVariantContext != null) {
					Genotype testGenotype = testVariantContext.getGenotype(sampleName);
					Genotype baselineGenotype = baselineVariantContext.getGenotype(sampleName);
					if(isVar(testGenotype) && isVar(baselineGenotype) && testGenotype.sameGenotype(baselineGenotype)){
						stat.get(sampleName)[2]++;
					}
					if(isVar(testGenotype))
						stat.get(sampleName)[0] ++;
					if(isVar(baselineGenotype))
						stat.get(sampleName)[1] ++;
				}else if(testVariantContext != null){
					Genotype gt = testVariantContext.getGenotype(sampleName);
					if(isVar(gt))
						stat.get(sampleName)[0] ++;
				}else if(baselineVariantContext != null){
					Genotype gt = baselineVariantContext.getGenotype(sampleName);
					if(isVar(gt))
						stat.get(sampleName)[1] ++;
				}
			}else {
				stat.put(sampleName, new int[3]);
			}
		}


	}

	public boolean isVar(Genotype gt){
		return  gt.isCalled() && !gt.isHomRef();
	}

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		resultValue.set("REF\t" + stat.get("Total")[0]+"\t"+stat.get("Total")[1] +"\t"+stat.get("Total")[2]);
		context.write(NullWritable.get(), resultValue);
		for (String sampleName : sampleNames) {
			if(!stat.containsKey(sampleName)){
				return;
			}
			resultValue.set(sampleName + "\t" + stat.get(sampleName)[0]+"\t"+stat.get(sampleName)[1] +"\t"+stat.get(sampleName)[2]);
			context.write(NullWritable.get(), resultValue);
		}
	}

}
