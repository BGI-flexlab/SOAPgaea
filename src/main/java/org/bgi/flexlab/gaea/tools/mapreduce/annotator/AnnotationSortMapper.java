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


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bgi.flexlab.gaea.data.mapreduce.writable.PairWritable;
import org.bgi.flexlab.gaea.tools.annotator.SampleAnnotationContext;
import org.bgi.flexlab.gaea.tools.annotator.VcfAnnoContext;
import org.bgi.flexlab.gaea.tools.annotator.config.Config;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AnnotationSortMapper extends Mapper<LongWritable, Text, PairWritable, Text> {

	private PairWritable resultKey;
	private Text resultValue;
	private AnnotatorOptions options;
	private Config userConfig;

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		resultKey = new PairWritable();
		resultValue = new Text();
		options = new AnnotatorOptions();
		Configuration conf = context.getConfiguration();
		options.getOptionsFromHadoopConf(conf);
		userConfig = new Config(conf);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String annoLine = value.toString();

		VcfAnnoContext vac = new VcfAnnoContext();
		vac.parseAnnotationStrings(annoLine, userConfig.getFields());
		String[] fields = annoLine.split("\t", 4);
		String secondKey = fields[1] + "-" + String.format("%09d",Integer.parseInt(fields[2]));

		for(SampleAnnotationContext sac: vac.getSampleAnnoContexts().values()){
			resultKey.set(sac.getSampleName(), secondKey);
			List<String> anno = new ArrayList<>();
			for(String field: userConfig.getFields()){
				String annoValue = sac.getFieldByName(field, sac.getSingleAlt());
				if (annoValue == null) {
					annoValue = vac.getAnnoItem(field);
				}
				anno.add(annoValue);
			}
			resultValue.set(String.join("\t",anno));
//			System.err.println("anno: " + String.join("\t",anno));
//			resultValue.set(vac.getAnnoStr()+"\t"+sac.toAlleleString(sac.getSingleAlt()));
			context.write(resultKey, resultValue);
		}
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {

	}
}
