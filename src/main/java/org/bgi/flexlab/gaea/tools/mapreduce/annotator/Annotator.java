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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.bgi.flexlab.gaea.data.structure.header.SingleVCFHeader;
import org.bgi.flexlab.gaea.data.structure.reference.ReferenceShare;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.BioJob;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.ToolsRunner;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

/**
 * Created by huangzhibo on 2017/7/7.
 */
public class Annotator extends ToolsRunner {

    public Annotator(){}



    public int runAnnotator(String[] arg0) throws Exception {

        Configuration conf = new Configuration();
        String[] remainArgs = remainArgs(arg0, conf);

        AnnotatorOptions options = new AnnotatorOptions();
        options.parse(remainArgs);
        options.setHadoopConf(remainArgs, conf);
        System.out.println("inputFilePath: "+conf.get("inputFilePath"));
        BioJob job = BioJob.getInstance(conf);

        if(options.isCachedRef())
            System.err.println("--------- isCachedRef --------");
        ReferenceShare.distributeCache(options.getReferenceSequencePath(), job);


        job.setHeader(new Path(options.getInput()), new Path(options.getOutput()));
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

        List<String> sampleNames = new ArrayList<>();

        Path inputPath = new Path(conf.get("inputFilePath"));
        FileSystem fs = inputPath.getFileSystem(conf);
        FileStatus[] files = fs.listStatus(inputPath);

        for(FileStatus file : files) {//统计sample names
            System.out.println(file.getPath());

            if (file.isFile()) {
                SingleVCFHeader singleVcfHeader = new SingleVCFHeader();
                singleVcfHeader.readHeaderFrom(file.getPath(), fs);
                VCFHeader vcfHeader = singleVcfHeader.getHeader();
                sampleNames.addAll(vcfHeader.getSampleNamesInOrder());
            }

        }

        MNLineInputFormat.addInputPath(job, new Path(options.getInputFilePath()));
        MNLineInputFormat.setMinNumLinesToSplit(job,1000); //按行处理的最小单位
        MNLineInputFormat.setMapperNum(job, options.getMapperNum());
        Path partTmp = new Path(options.getTmpPath());

        FileOutputFormat.setOutputPath(job, partTmp);
        for(int i = 0; i < sampleNames.size(); i++)//相同sample name输出到同一个文件夹
        {
            System.out.println("sampleName "+i+":"+SampleNameModifier.modify(sampleNames.get(i)));
            MultipleOutputs.addNamedOutput(job, SampleNameModifier.modify(sampleNames.get(i)), TextOutputFormat.class, NullWritable.class, Text.class);
        }
        if (job.waitForCompletion(true)) {
            for(int i = 0; i < sampleNames.size(); i++) {//同一个文件夹下的结果整合到一个文件
                GZIPOutputStream os = new GZIPOutputStream(new FileOutputStream(options.getOutputPath()+ "/" + sampleNames.get(i) + ".tsv.gz"));
                final FileStatus[] parts = partTmp.getFileSystem(conf).globStatus(new Path(options.getTmpPath() + "/" +sampleNames.get(i) +
                        "/part" + "-*-[0-9][0-9][0-9][0-9][0-9]*"));
                boolean writeHeader = true;
                for (FileStatus p : parts) {
                    FSDataInputStream dis = p.getPath().getFileSystem(conf).open(p.getPath());
                    BufferedReader reader = new BufferedReader(new InputStreamReader(dis));
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (line.startsWith("#")) {
                            if (writeHeader) {
                                os.write(line.getBytes());
                                os.write('\n');
                                writeHeader = false;
                            }
                            continue;
                        }
                        os.write(line.getBytes());
                        os.write('\n');
                    }
                }
                os.close();
            }
            partTmp.getFileSystem(conf).delete(partTmp, true);

            return 0;
        }else {
            return 1;
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        Annotator md = new Annotator();
        return md.runAnnotator(args);
    }

}
