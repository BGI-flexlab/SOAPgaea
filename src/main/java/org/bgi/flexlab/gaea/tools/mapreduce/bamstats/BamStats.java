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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.BioJob;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.ToolsRunner;
import org.bgi.flexlab.gaea.tools.bamstats.BamReport;
import org.seqdoop.hadoop_bam.SAMFormat;

import java.util.concurrent.TimeUnit;


public class BamStats extends ToolsRunner {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] remainArgs = remainArgs(args, conf);

        BamStatsOptions options = new BamStatsOptions();
        options.parse(remainArgs);
        options.setHadoopConf(remainArgs, conf);
        BioJob job = BioJob.getInstance(conf);

        job.setJobName("GaeaBamStats");
        job.setJarByClass(this.getClass());
        job.setMapperClass(BamStatsMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setAnySamInputFormat(SAMFormat.BAM);

        FileInputFormat.setInputPaths(job, options.getInputsAsArray());

        Path partTmp = new Path(options.getOutputPath() + "/out_temp");
        FileOutputFormat.setOutputPath(job, partTmp);

        if (job.waitForCompletion(true)) {
            int loop = 0;
            while (!partTmp.getFileSystem(conf).exists(partTmp) && loop < 30){
                TimeUnit.MILLISECONDS.sleep(1000);
                loop ++;
            }

            BamReport report = new BamReport(options);
            report.mergeReport(partTmp, conf,
                    new Path(options.getOutputPath()));
            partTmp.getFileSystem(conf).delete(partTmp, true);
            return 0;
        } else {
            return 1;
        }
    }

}
