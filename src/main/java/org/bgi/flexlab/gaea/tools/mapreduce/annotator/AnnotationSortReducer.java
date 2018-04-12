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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.bgi.flexlab.gaea.data.mapreduce.writable.PairWritable;
import org.bgi.flexlab.gaea.tools.annotator.config.Config;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class AnnotationSortReducer extends Reducer<PairWritable, Text, NullWritable, Text> {

	private MultipleOutputs<NullWritable,Text> multipleOutputs = null;
	private Text resultValue;
	private Set<String> printHeader = new HashSet<>();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		resultValue = new Text();
		multipleOutputs = new MultipleOutputs<>(context);
		Configuration conf = context.getConfiguration();
		Config userConfig = new Config(conf);
		System.err.println(userConfig.getHeaderString());
		resultValue.set(userConfig.getHeaderString());
	}

	@Override
	protected void reduce(PairWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		if(!printHeader.contains(key.getFirst())) {
			multipleOutputs.write(NullWritable.get(), resultValue, key.getFirst());
			printHeader.add(key.getFirst());
		}
		for (Text inputLine : values) {
			resultValue.set(inputLine);
			multipleOutputs.write(NullWritable.get(), resultValue, key.getFirst());
		}
	}

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		multipleOutputs.close();
	}
}
