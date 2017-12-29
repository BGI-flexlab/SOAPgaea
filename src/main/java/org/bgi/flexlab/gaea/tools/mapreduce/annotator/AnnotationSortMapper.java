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
import org.bgi.flexlab.gaea.tools.annotator.SampleAnnotationContext;
import org.bgi.flexlab.gaea.tools.annotator.VcfAnnoContext;

import java.io.IOException;

public class AnnotationSortMapper extends Mapper<LongWritable, Text, PairWritable, Text> {

	private PairWritable resultKey;
	private Text resultValue;
	private Configuration conf;
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		resultKey = new PairWritable();
		resultValue = new Text();
		conf = context.getConfiguration();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String annoLine = value.toString();
		if (annoLine.startsWith("#")) return;

		VcfAnnoContext vac = new VcfAnnoContext();
		vac.parseAnnotationStrings(annoLine);
		String[] fields = annoLine.split("\t", 3);
		String secondKey = fields[0] + "-" + String.format("%09d",Integer.parseInt(fields[1]));

		for(SampleAnnotationContext sac: vac.getSampleAnnoContexts().values()){
			resultKey.set(sac.getSampleName(), secondKey);
			resultValue.set(vac.getAnnoStr()+"\t"+sac.toAlleleString(sac.getSingleAlt()));
			context.write(resultKey, resultValue);
		}
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {

	}
}
