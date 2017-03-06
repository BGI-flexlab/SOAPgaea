package org.bgi.flexlab.gaea.tools.mapreduce.genotyper;

import htsjdk.variant.vcf.VCFHeader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.bgi.flexlab.gaea.data.mapreduce.output.vcf.GaeaVCFOutputFormat;
import org.bgi.flexlab.gaea.data.mapreduce.output.vcf.VCFHdfsWriter;
import org.bgi.flexlab.gaea.data.mapreduce.writable.SamRecordWritable;
import org.bgi.flexlab.gaea.data.mapreduce.writable.WindowsBasedWritable;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.BioJob;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.ToolsRunner;
import org.bgi.flexlab.gaea.framework.tools.mapreduce.WindowsBasedMapper;
import org.bgi.flexlab.gaea.tools.genotyer.VariantCallingEngine;
import org.bgi.flexlab.gaea.tools.genotyer.annotator.VariantAnnotatorEngine;
import org.seqdoop.hadoop_bam.KeyIgnoringVCFOutputFormat;
import org.seqdoop.hadoop_bam.VariantContextWritable;

import java.io.IOException;

/**
 * Created by zhangyong on 2017/3/3.
 */
public class Genotyper extends ToolsRunner {

    public Genotyper() {
        this.toolsDescription = "Gaea genotyper";
    }

    private GenotyperOptions options = null;

    private int runRealigner(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BioJob job = BioJob.getInstance();
        Configuration conf = job.getConfiguration();
        String[] remainArgs = remainArgs(args, conf);

        options = new GenotyperOptions();
        options.parse(remainArgs);
        options.setHadoopConf(remainArgs, conf);
        // merge header and set to configuration
        job.setHeader(new Path(options.getInput()), new Path(options.getBAMHeaderOutput()));

        //vcf header
        conf.set(GaeaVCFOutputFormat.OUT_PATH_PROP, options.getVCFHeaderOutput());
        VariantAnnotatorEngine variantAnnotatorEngine = new VariantAnnotatorEngine(options.getAnnotationGroups(), options.getAnnotations(), null);
        VCFHeader vcfHeader = VariantCallingEngine.getVCFHeader(options, variantAnnotatorEngine);
        VCFHdfsWriter vcfHdfsWriter = new VCFHdfsWriter(GaeaVCFOutputFormat.OUT_PATH_PROP, false, false, conf);
        vcfHdfsWriter.writeHeader(vcfHeader);

        job.setJobName("GaeaGenotyper");
        job.setAnySamInputFormat(options.getInputFormat());
        conf.set(KeyIgnoringVCFOutputFormat.OUTPUT_VCF_FORMAT_PROPERTY, options.getOuptputFormat().toString());
        job.setOutputFormatClass(GaeaVCFOutputFormat.class);
        job.setOutputKeyValue(WindowsBasedWritable.class, SamRecordWritable.class, NullWritable.class, VariantContextWritable.class);

        job.setJarByClass(Genotyper.class);
        job.setWindowsBasicMapperClass(WindowsBasedMapper.class, options.getWindowSize());
        job.setReducerClass(GenotyperReducer.class);
        job.setNumReduceTasks(options.getReducerNumber());

        FileInputFormat.setInputPaths(job, new Path(options.getInput()));
        FileOutputFormat.setOutputPath(job, new Path(options.getOutput()));

        if (job.waitForCompletion(true)) {
            return 0;
        }

        return 1;
    }


    @Override
    public int run(String[] args) throws Exception {
        Genotyper genotyper = new Genotyper();
        int res = genotyper.run(args);
        if(res != 0) {
            throw new RuntimeException("GaeaGenotyper Failed!");
        }

        return res;
    }

}
