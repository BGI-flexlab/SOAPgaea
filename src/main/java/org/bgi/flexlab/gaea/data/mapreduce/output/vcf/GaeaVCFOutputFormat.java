package org.bgi.flexlab.gaea.data.mapreduce.output.vcf;

import hbparquet.hadoop.util.ContextUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.seqdoop.hadoop_bam.KeyIgnoringVCFOutputFormat;
import org.seqdoop.hadoop_bam.VariantContextWritable;

import java.io.IOException;

/**
 * Created by zhangyong on 2017/3/3.
 * came form VCFsort
 */
public class GaeaVCFOutputFormat<K> extends FileOutputFormat<K, VariantContextWritable> {

    public static final String OUT_PATH_PROP = "gaea.vcf.outpath";

    private KeyIgnoringVCFOutputFormat<K> baseOF;

    private void initBaseOF(Configuration conf) {
        if (baseOF == null)
            baseOF = new KeyIgnoringVCFOutputFormat<K>(conf);
    }

    @Override public RecordWriter<K,VariantContextWritable> getRecordWriter(
            TaskAttemptContext context)
            throws IOException {
        final Configuration conf = ContextUtil.getConfiguration(context);
        initBaseOF(conf);
        if (baseOF.getHeader() == null) {
            final Path p = new Path(conf.get(OUT_PATH_PROP));
            baseOF.readHeaderFrom(p, p.getFileSystem(conf));
        }

        return baseOF.getRecordWriter(context, getDefaultWorkFile(context, ""));
    }

    // Allow the output directory to exist.
    @Override public void checkOutputSpecs(JobContext job) {}
}
