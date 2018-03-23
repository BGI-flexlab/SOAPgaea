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

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMReadGroupRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.BioJob;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.ToolsRunner;
import org.seqdoop.hadoop_bam.SAMRecordWritable;
import org.seqdoop.hadoop_bam.cli.Utils;
import org.seqdoop.hadoop_bam.util.Timer;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BamSort extends ToolsRunner {

    BamSortOptions options;
    final String intermediateOutName = "hadoopBamSort";
    private SAMFileHeader header;
    private Map<String, String> formatSampleName;

    public BamSort(){
        this.toolsDescription = "Bam BamSort";
    }


    public int runSort(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        BioJob job = BioJob.getInstance();
        Configuration conf = job.getConfiguration();
        String[] remainArgs = remainArgs(args, conf);
        options = new BamSortOptions();
        options.parse(remainArgs);

        Path tmpPath = new Path(options.getTmpPath());

        header = BamSortUtils.getHeaderMerger(conf).getMergedHeader();
        header.setSortOrder(SAMFileHeader.SortOrder.coordinate);

        if (header.getReadGroups().size() == 1) {
            options.setMultiSample(false);
        }

        options.setHadoopConf(remainArgs, conf);

        if (!options.isMultiSample())
            Utils.configureSampling(tmpPath, intermediateOutName, conf);

        final List<Path> inputs = new ArrayList<>(options.getStrInputs().size());
        for (final String in : options.getStrInputs())
            inputs.add(new Path(in));

//        job.setHeader(new Path(options.getInput()), new Path(options.getOutput()));

        job.setJarByClass(BamSort.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);
        job.setJobName("multi sample sort");

        job.setMapOutputKeyClass(LongWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(SAMRecordWritable.class);

        Timer t = new Timer();
        formatSampleName = new HashMap<>();

        FileOutputFormat.setOutputPath(job, tmpPath);
        if(options.getRenames() != null){
            header = BamSortUtils.replaceSampleName(header.clone(), options.getRenames());
        }

        for (SAMReadGroupRecord rg : header.getReadGroups()) {
            String fsn = BamSortUtils.formatSampleName(rg.getSample());
            if (!formatSampleName.containsKey(fsn)) {
                formatSampleName.put(fsn, rg.getSample());
                SortMultiOutputs.addNamedOutput(job, fsn,
                        SortOutputFormat.class, NullWritable.class,
                        SAMRecordWritable.class);
            }
            if (!options.isMultiSample())
                break;
        }

        if (!options.isMultiSample()) {
            System.out.println("sort :: Sampling...");
            t.start();

            job.setPartitionerClass(TotalOrderPartitioner.class);
            InputSampler
                    .<LongWritable, SAMRecordWritable> writePartitionFile(
                            job,
                            new InputSampler.RandomSampler<LongWritable, SAMRecordWritable>(
                                    0.01, 10000, Math.max(100, options.getReducerNum())));
            System.out.printf("sort :: Sampling complete in %d.%03d s.\n",
                    t.stopS(), t.fms());
            String partitionFile = TotalOrderPartitioner.getPartitionFile(job.getConfiguration());
            URI partitionUri = new URI(partitionFile + "#" + TotalOrderPartitioner.DEFAULT_PATH);
            job.addCacheFile(partitionUri);
        } else {
            job.setPartitionerClass(SamSortPartition.class);
        }

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    @Override
    public int run(String[] args) throws Exception {
        BamSort sort = new BamSort();
        return sort.runSort(args);
    }
}