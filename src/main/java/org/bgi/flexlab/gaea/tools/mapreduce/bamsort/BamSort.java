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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.bgi.flexlab.gaea.data.mapreduce.input.header.SamHdfsFileHeader;
import org.bgi.flexlab.gaea.data.mapreduce.output.bam.GaeaBamOutputFormat;
import org.bgi.flexlab.gaea.data.mapreduce.partitioner.FirstPartitioner;
import org.bgi.flexlab.gaea.data.mapreduce.writable.PairWritable;
import org.bgi.flexlab.gaea.data.mapreduce.writable.SamRecordWritable;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.BioJob;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.ToolsRunner;
import org.seqdoop.hadoop_bam.cli.Utils;
import org.seqdoop.hadoop_bam.util.Timer;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class BamSort extends ToolsRunner {

    BamSortOptions options;
    final String intermediateOutName = "hadoopBamSort";
    private SAMFileHeader header;
    private Map<String, String> formatSampleName;
    private List<String> sampleNames;
    BioJob job;
    Configuration conf;
    List<Path> inputs;
    Path tmpPath;
    Path outputPath;

    public BamSort(){
        this.toolsDescription = "Gaea BamSort";
        sampleNames = new ArrayList<>();
    }


    public int runSort(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        job = BioJob.getInstance();
        conf = job.getConfiguration();
        String[] remainArgs = remainArgs(args, conf);
        options = new BamSortOptions();
        options.parse(remainArgs);
        tmpPath = new Path(options.getTmpPath());
        outputPath = new Path(options.getOutdir());
        options.setHadoopConf(remainArgs, conf);

        inputs = new ArrayList<>();
        for (String in : options.getStrInputs())
            inputs.add(new Path(in));
        job.setHeader(inputs, outputPath);

        header = SamHdfsFileHeader.getHeader(conf);
        if(header == null){
            System.err.println("Header is null!");
            System.exit(-1);
        }

        for (SAMReadGroupRecord rg : header.getReadGroups()) {
            if(!sampleNames.contains(rg.getSample()))
                sampleNames.add(rg.getSample());
        }

        if(options.isMultiSample())
            return runMultiSort();

        return runSingleSort();
    }

    public int runSingleSort() throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        if (!options.isMultiSample())
            Utils.configureSampling(tmpPath, intermediateOutName, conf);

        conf.setBoolean(SortOutputFormat.WRITE_HEADER_PROP, options.getOutdir() == null);
        conf.set(SortOutputFormat.OUTPUT_NAME_PROP, intermediateOutName);
        conf.set(SortOutputFormat.OUTPUT_SAM_FORMAT_PROPERTY, options.getOutputFormat());

        if (header.getReadGroups().size() == 1) {
            options.setMultiSample(false);
        }
        job.setJarByClass(BamSort.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);
        job.setJobName("bamsort");

        job.setMapOutputKeyClass(LongWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(SamRecordWritable.class);

        job.setAnySamInputFormat(options.getInputFormat());
        job.setOutputFormatClass(SortOutputFormat.class);

        Timer t = new Timer();
        formatSampleName = new HashMap<>();

        for (final Path in : inputs)
            FileInputFormat.addInputPath(job, in);

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
                        SamRecordWritable.class);
            }
            if (!options.isMultiSample())
                break;
        }

        if (!options.isMultiSample()) {
            System.out.println("sort :: Sampling...");
            t.start();

            job.setPartitionerClass(TotalOrderPartitioner.class);
            InputSampler.writePartitionFile(
                    job,
                    new InputSampler.RandomSampler<LongWritable, SamRecordWritable>(
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

    public int runMultiSort() throws IOException, ClassNotFoundException, InterruptedException {
        job.setJarByClass(BamSort.class);
        job.setMapperClass(MultiSortMapper.class);
        job.setReducerClass(MultiSortReducer.class);
        job.setJobName("multi bamsort");
        job.setNumReduceTasks(sampleNames.size());

        job.setMapOutputKeyClass(PairWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(SamRecordWritable.class);

        job.setAnySamInputFormat(options.getInputFormat());
        LazyOutputFormat.setOutputFormatClass(job, GaeaBamOutputFormat.class);

        for (Path in : inputs)
            FileInputFormat.addInputPath(job, in);

        FileOutputFormat.setOutputPath(job, tmpPath);

        job.setPartitionerClass(FirstPartitioner.class);

        FileSystem fs = tmpPath.getFileSystem(conf);
        if(job.waitForCompletion(true)){
            int loop = 0;
            for (String fileName : sampleNames){
                Path outputPart = getSampleOutputPath(fileName);
                while (outputPart == null && loop < 10){
                    TimeUnit.MILLISECONDS.sleep(6000);
                    outputPart = getSampleOutputPath(fileName);
                    loop ++;
                }
                Path outputName = new Path(options.getOutdir() + "/" + fileName+".bam");
                fs.rename(outputPart, outputName);
            }
            fs.delete(tmpPath, true);
            return 0;
        }
        return 1;
    }

    private Path getSampleOutputPath(String sample) throws IOException {
        FileSystem fs = tmpPath.getFileSystem(conf);
        FileStatus[] fileStatuses = fs.globStatus(new Path(options.getTmpPath() + "/" + sample + "-r-[0-9]*"));
        if(fileStatuses.length == 0){
            System.err.println(sample+": cann't get the output part file!");
            FileStatus[] fss = fs.globStatus(new Path(options.getTmpPath() + "/*"));
            for (FileStatus f: fss){
                System.err.println("OutPath" + f.getPath().toString());
            }
            return null;
        }
        return fileStatuses[0].getPath();
    }

    @Override
    public int run(String[] args) throws Exception {
        BamSort sort = new BamSort();
        return sort.runSort(args);
    }
}