package org.bgi.flexlab.gaea.tools.mapreduce.vcfstats;

import htsjdk.variant.variantcontext.VariantContext;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bgi.flexlab.gaea.tools.vcfstats.report.PerSampleVCFReport;
import org.bgi.flexlab.gaea.tools.vcfstats.report.VCFReport;
import org.seqdoop.hadoop_bam.VariantContextWritable;

import java.io.IOException;

public class VCFStatsMapper extends Mapper<LongWritable, VariantContextWritable, NullWritable, Text> {

    private VCFReport vcfReport;
    private Text resultValue = new Text();
    protected void setup(Context context)
            throws IOException, InterruptedException {
        VCFStatsOptions options = new VCFStatsOptions();
        options.getOptionsFromHadoopConf(context.getConfiguration());
        vcfReport = new VCFReport(options);
    }

    @Override
    protected void map(LongWritable key, VariantContextWritable value, Context context) throws IOException, InterruptedException {
        VariantContext vc = value.get();
        vcfReport.parseVariation(vc);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for(String sample: vcfReport.getPerSampleVCFReports().keySet()){
            PerSampleVCFReport sampleVCFReport = vcfReport.getPerSampleVCFReports().get(sample);
            resultValue.set(sampleVCFReport.toReducerString());
            context.write(NullWritable.get(), resultValue);
        }
    }
}
