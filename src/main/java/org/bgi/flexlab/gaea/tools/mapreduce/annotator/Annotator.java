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

import htsjdk.samtools.util.BlockCompressedStreamConstants;
import htsjdk.variant.vcf.VCFHeader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.bgi.flexlab.gaea.data.mapreduce.partitioner.FirstPartitioner;
import org.bgi.flexlab.gaea.data.mapreduce.writable.PairWritable;
import org.bgi.flexlab.gaea.data.mapreduce.writable.VcfLineWritable;
import org.bgi.flexlab.gaea.data.structure.header.SingleVCFHeader;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.BioJob;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.ToolsRunner;
import org.seqdoop.hadoop_bam.VCFOutputFormat;
import org.seqdoop.hadoop_bam.util.BGZFCodec;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Annotator extends ToolsRunner {
    
    private Configuration conf;
    private AnnotatorOptions options;
    private List<String> sampleNames;
    private List<String> fileNames;
    private String sortInput;
    private Path inputPath;
    private Path outputPath;

    public Annotator(){
        sampleNames = new ArrayList<>();
        fileNames = new ArrayList<>();
    }

    public Annotator(String[] arg0) throws IOException {
        sampleNames = new ArrayList<>();
        fileNames = new ArrayList<>();

        conf = new Configuration();
        String[] remainArgs = remainArgs(arg0, conf);

        options = new AnnotatorOptions();
        options.parse(remainArgs);
        options.setHadoopConf(remainArgs, conf);

        sortInput = options.getInputFilePath();

        outputPath = new Path(options.getOutputPath());
        if(outputPath.getFileSystem(conf).exists(outputPath)){
            throw new RuntimeException("Output dir is exists: " + outputPath);
        }

    }

    public AnnotatorOptions getOptions() {
        return options;
    }

    private int runAnnotator() throws Exception {

        inputPath = new Path(options.getInputFilePath());
        FileSystem fs = inputPath.getFileSystem(conf);
        FileStatus[] files = fs.listStatus(inputPath);

        for(FileStatus file : files) {//统计sample names
            System.out.println(file.getPath());
            if (file.isFile()) {
                SingleVCFHeader singleVcfHeader = new SingleVCFHeader();
                singleVcfHeader.readHeaderFrom(file.getPath(), fs);
                VCFHeader vcfHeader = singleVcfHeader.getHeader();

                fileNames.add(file.getPath().getName());

                for(String sample: vcfHeader.getSampleNamesInOrder()) {
                    if(!sampleNames.contains(sample))
                        sampleNames.add(sample);
                }
            }
        }

        conf.set(VCFOutputFormat.OUTPUT_VCF_FORMAT_PROPERTY, "VCF");
        BioJob job = BioJob.getInstance(conf);

        job.setJobName("GaeaAnnotator");
        job.setJarByClass(this.getClass());
        job.setMapperClass(AnnotationMapper.class);
        job.setReducerClass(AnnotationReducer.class);
        job.setNumReduceTasks(options.getReducerNum());

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(VcfLineWritable.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


//        if(sampleNames.size() <= 1){
//            job.setInputFormatClass(MNLineInputFormat.class);
//            MNLineInputFormat.addInputPath(job, new Path(options.getInputFilePath()));
//            MNLineInputFormat.setMinNumLinesToSplit(job,1000); //按行处理的最小单位
//            MNLineInputFormat.setMapperNum(job, options.getReducerNum());
//        }else {
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.addInputPath(job, inputPath);
//        }

        if(options.getRunStep() == AnnotatorOptions.RunStep.ANN){
            FileOutputFormat.setOutputPath(job, outputPath);
        }else {
            Path partTmp = new Path(options.getTmpPath());
            FileOutputFormat.setOutputPath(job, partTmp);
            sortInput = options.getTmpPath();
        }

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private int runAnnoSort() throws Exception {
        if(options.getOutputFormat() == AnnotatorOptions.OutputFormat.VCF){
            return runVCFSort();
        }
        return runTSVSort();
    }

    private int runVCFSort() throws Exception {

        conf.set("io.compression.codecs", BGZFCodec.class.getCanonicalName());
        BioJob job = BioJob.getInstance(conf);

        job.setJobName("GaeaAnnotatorSort");
        job.setJarByClass(this.getClass());
        job.setMapperClass(AnnoSortMapper.class);
        job.setReducerClass(AnnoSortReducer.class);
        job.setNumReduceTasks(fileNames.size());

        job.setMapOutputKeyClass(PairWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setPartitionerClass(FirstPartitioner.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        Path inputPath = new Path(sortInput);
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, BGZFCodec.class);

        FileSystem fs = outputPath.getFileSystem(conf);
        if(job.waitForCompletion(true)){
            int loop = 0;
            for (String fileName : fileNames){
                Path outputPart = getSampleOutputPath(fileName);
                while (outputPart == null && loop < 10){
                    TimeUnit.MILLISECONDS.sleep(6000);
                    outputPart = getSampleOutputPath(fileName);
                    loop ++;
                }
                Path outputName = new Path(options.getOutputPath() + "/" + fileName);
                fs.rename(outputPart, outputName);
            }
            return 0;
        }
        return 1;
    }


    private int runTSVSort() throws Exception {

        conf.set("io.compression.codecs", BGZFCodec.class.getCanonicalName());
        BioJob job = BioJob.getInstance(conf);

        job.setJobName("GaeaAnnotatorSort");
        job.setJarByClass(this.getClass());
        job.setMapperClass(AnnotationSortMapper.class);
        job.setReducerClass(AnnotationSortReducer.class);
        if(options.getRunStep() == AnnotatorOptions.RunStep.SORT)
            job.setNumReduceTasks(options.getReducerNum());
        else
            job.setNumReduceTasks(sampleNames.size());

        job.setMapOutputKeyClass(PairWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setPartitionerClass(FirstPartitioner.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        Path inputPath = new Path(sortInput);
        Path outputPath = new Path(options.getOutputPath());
        FileInputFormat.setInputPaths(job, inputPath);
        FileInputFormat.setMinInputSplitSize(job, 268435456);
        FileOutputFormat.setOutputPath(job, outputPath);
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, BGZFCodec.class);

        FileSystem fs = outputPath.getFileSystem(conf);
        if(job.waitForCompletion(true)){
            int loop = 0;
            FileStatus[] fileStatuses = fs.globStatus(new Path(options.getOutputPath() + "/*-r-[0-9]*"));
            while (fileStatuses.length == 0 && loop < 10){
                TimeUnit.MILLISECONDS.sleep(6000);
                fileStatuses = fs.globStatus(new Path(options.getOutputPath() + "/*-r-[0-9]*"));
                loop ++;
            }

            for(FileStatus fstat: fileStatuses){
                String sample = fstat.getPath().getName().split("-r-")[0];
                Path outputName = new Path(options.getOutputPath() + "/" + sample + ".tsv.gz");
                OutputStream outgz = fs.create(outputName);
                final FSDataInputStream ins = fs.open(fstat.getPath());
                IOUtils.copyBytes(ins, outgz, conf, false);
                ins.close();
                outgz.write(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK);
                outgz.close();
                fs.deleteOnExit(fstat.getPath());
            }

//            for (String sampleName : sampleNames){
//                Path outputPart = getSampleOutputPath(sampleName);
//                while (outputPart == null && loop < 10){
//                    TimeUnit.MILLISECONDS.sleep(6000);
//                    outputPart = getSampleOutputPath(sampleName);
//                    loop ++;
//                }
//                Path outputName = new Path(options.getOutputPath() + "/" + sampleName + ".tsv.gz");
//                OutputStream outgz = fs.create(outputName);
//                final FSDataInputStream ins = fs.open(outputPart);
//                IOUtils.copyBytes(ins, outgz, conf, false);
//                ins.close();
//                outgz.write(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK);
//                outgz.close();
//                fs.deleteOnExit(outputPart);
//            }
//            inputPath.getFileSystem(conf).delete(inputPath, true);
            return 0;
        }
        return 1;
    }

    private Path getSampleOutputPath(String sample) throws IOException {
        Path outputPath = new Path(options.getOutputPath());
        FileSystem fs = outputPath.getFileSystem(conf);
        FileStatus[] fileStatuses = fs.globStatus(new Path(options.getOutputPath() + "/" + sample + "-r-[0-9]*"));
        if(fileStatuses.length == 0){
            System.err.println(sample+": cann't get the output part file!");
            FileStatus[] fss = fs.globStatus(new Path(options.getOutputPath() + "/*"));
            for (FileStatus f: fss){
                System.err.println("OutPath" + f.getPath().toString());
            }
            return null;
        }
        return fileStatuses[0].getPath();
    }

    @Override
    public int run(String[] args) throws Exception {
        Annotator annotator = new Annotator(args);
        if(annotator.getOptions().getRunStep() == AnnotatorOptions.RunStep.SORT)
            return annotator.runAnnoSort();
        else if(annotator.getOptions().getRunStep() == AnnotatorOptions.RunStep.ANN)
            return annotator.runAnnotator() == 0 ? 0 : 1;

        return annotator.runAnnotator() == 0 ? annotator.runAnnoSort() : 1;
    }

}
