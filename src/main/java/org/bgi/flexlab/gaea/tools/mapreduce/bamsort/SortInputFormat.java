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
package org.bgi.flexlab.gaea.tools.mapreduce.bamsort;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.seqdoop.hadoop_bam.AnySAMInputFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

public final class SortInputFormat extends
		FileInputFormat<LongWritable, SAMRecordWritable> {
	private AnySAMInputFormat baseIF = null;

	private void initBaseIF(final Configuration conf) {
		if (baseIF == null)
			baseIF = new AnySAMInputFormat(conf);
	}

	@Override
	public RecordReader<LongWritable, SAMRecordWritable> createRecordReader(
			InputSplit split, TaskAttemptContext ctx)
			throws InterruptedException, IOException {
		initBaseIF(ctx.getConfiguration());

		final RecordReader<LongWritable, SAMRecordWritable> rr = new SortRecordReader(
				baseIF.createRecordReader(split, ctx));
		rr.initialize(split, ctx);
		return rr;
	}

	@Override
	protected boolean isSplitable(JobContext job, Path path) {
		initBaseIF(job.getConfiguration());
		return baseIF.isSplitable(job, path);
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		initBaseIF(job.getConfiguration());
		return baseIF.getSplits(job);
	}
}