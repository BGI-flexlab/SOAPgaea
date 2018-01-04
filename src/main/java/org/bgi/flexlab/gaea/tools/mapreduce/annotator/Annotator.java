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

import htsjdk.variant.vcf.VCFHeader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.bgi.flexlab.gaea.data.structure.header.SingleVCFHeader;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.BioJob;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.ToolsRunner;

import java.util.ArrayList;
import java.util.List;

public class Annotator extends ToolsRunner {
    
    private Configuration conf;
    private AnnotatorOptions options;
    private List<String> sampleNames;

    public Annotator(){}

    private int runAnnotator(String[] arg0) throws Exception {

        conf = new Configuration();
        String[] remainArgs = remainArgs(arg0, conf);

        options = new AnnotatorOptions();
        options.parse(remainArgs);
        options.setHadoopConf(remainArgs, conf);
        BioJob job = BioJob.getInstance(conf);

        job.setJobName("GaeaAnnotator");
        job.setJarByClass(this.getClass());
        job.setMapperClass(AnnotationMapper.class);
        job.setReducerClass(AnnotationReducer.class);
        job.setNumReduceTasks(options.getReducerNum());

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(VcfLineWritable.class);


        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(MNLineInputFormat.class);

        sampleNames = new ArrayList<>();

        Path inputPath = new Path(conf.get("inputFilePath"));
        FileSystem fs = inputPath.getFileSystem(conf);
        FileStatus[] files = fs.listStatus(inputPath);

        for(FileStatus file : files) {//统计sample names
            System.out.println(file.getPath());
            if (file.isFile()) {
                SingleVCFHeader singleVcfHeader = new SingleVCFHeader();
                singleVcfHeader.readHeaderFrom(file.getPath(), fs);
                VCFHeader vcfHeader = singleVcfHeader.getHeader();
                for(String sample: vcfHeader.getSampleNamesInOrder()) {
                    if(!sampleNames.contains(sample))
                        sampleNames.add(sample);
                }
            }
        }

        MNLineInputFormat.addInputPath(job, new Path(options.getInputFilePath()));
        MNLineInputFormat.setMinNumLinesToSplit(job,1000); //按行处理的最小单位
        MNLineInputFormat.setMapperNum(job, options.getReducerNum());
        Path partTmp = new Path(options.getTmpPath());

        FileOutputFormat.setOutputPath(job, partTmp);

        return job.waitForCompletion(true) ? 0 : 1;
    }


    private int runAnnotatorSort() throws Exception {

        BioJob job = BioJob.getInstance(conf);

        job.setJobName("GaeaAnnotatorSortResult");
        job.setJarByClass(this.getClass());
        job.setMapperClass(AnnotationSortMapper.class);
        job.setReducerClass(AnnotationSortReducer.class);
        job.setNumReduceTasks(sampleNames.size());

        job.setMapOutputKeyClass(PairWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        Path inputPath = new Path(options.getTmpPath());
        FileSystem fs = inputPath.getFileSystem(conf);
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, new Path(options.getOutputPath()));

        if(job.waitForCompletion(true)){
            for (String sampleName : sampleNames){
                FileStatus[] fileStatuses = fs.globStatus(new Path(options.getOutputPath() + "/" + sampleName + "-r-*"));
                if(fileStatuses.length>1)
                    System.err.println(sampleName+": rename result file error!"); ;
                Path outputName = new Path(options.getOutputPath() + "/" + sampleName + ".tsv");
                fs.rename(fileStatuses[0].getPath(), outputName);
            }
            return 0;
        }
        return 1;
    }


    @Override
    public int run(String[] args) throws Exception {
        Annotator annotator = new Annotator();
        if(annotator.runAnnotator(args) != 0)
            return 1;
        return annotator.runAnnotatorSort();
    }

}
