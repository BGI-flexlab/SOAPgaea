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
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderVersion;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.bgi.flexlab.gaea.data.structure.header.SingleVCFHeader;
import org.bgi.flexlab.gaea.data.structure.reference.ReferenceShare;
import org.bgi.flexlab.gaea.tools.annotator.AnnotationEngine;
import org.bgi.flexlab.gaea.tools.annotator.SampleAnnotationContext;
import org.bgi.flexlab.gaea.tools.annotator.VcfAnnoContext;
import org.bgi.flexlab.gaea.tools.annotator.VcfAnnotator;
import org.bgi.flexlab.gaea.tools.annotator.config.Config;
import org.bgi.flexlab.gaea.tools.annotator.db.DBAnnotator;

import java.io.IOException;
import java.util.*;

public class AnnotationSortReducer extends Reducer<Text, Text, NullWritable, Text> {

	private Text resultValue;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		resultValue = new Text();
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		long start = System.currentTimeMillis();
		Iterator<Text> iter =  values.iterator();
		while(iter.hasNext()) {
			Text inputLine = iter.next();
			resultValue.set(inputLine);
			context.write(NullWritable.get(), resultValue);
		}
	}

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
	}
}
