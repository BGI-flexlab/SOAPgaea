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
package org.bgi.flexlab.gaea.tools.mapreduce.bamstats;

import htsjdk.samtools.SAMRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bgi.flexlab.gaea.data.mapreduce.writable.SamRecordWritable;
import org.bgi.flexlab.gaea.tools.bamstats.BamReport;

import java.io.IOException;


public class BamStatsMapper extends Mapper<LongWritable, SamRecordWritable, NullWritable, Text> {
    private BamReport bamReport;
    private Text resultValue = new Text();

    @Override
    public void setup(Context context) {
        Configuration conf = context.getConfiguration();
//        SAMFileHeader header = SamHdfsFileHeader.getHeader(conf);
        BamStatsOptions options = new BamStatsOptions();
        options.getOptionsFromHadoopConf(conf);
        bamReport = new BamReport(options);
    }

    @Override
    public void map(LongWritable key, SamRecordWritable value, Context context) throws IOException, InterruptedException {
//        if (options.isRemoveSecond() && value.get().isSecondaryOrSupplementary())
//            return;
        SAMRecord samRecord = value.get();
        bamReport.add(samRecord);
    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        resultValue.set(bamReport.toReducerString());
        context.write(NullWritable.get(), resultValue);
    }

}
