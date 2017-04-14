package org.bgi.flexlab.gaea.tools.mapreduce.markduplicate;

import htsjdk.samtools.SAMFileHeader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.bgi.flexlab.gaea.data.mapreduce.input.header.SamHdfsFileHeader;
import org.bgi.flexlab.gaea.data.mapreduce.writable.CreateDuplicationKey;
import org.bgi.flexlab.gaea.data.mapreduce.writable.DuplicationKeyWritable;
import org.bgi.flexlab.gaea.data.mapreduce.writable.SamRecordWritable;

import java.io.IOException;

public class MarkDuplicateMappper extends Mapper<LongWritable, SamRecordWritable, DuplicationKeyWritable, SamRecordWritable> {
    private CreateDuplicationKey bamKey;
    private DuplicationKeyWritable dupKey;

    @Override
    public void setup(Context context) {
        Configuration conf = context.getConfiguration();
        SAMFileHeader header = SamHdfsFileHeader.getHeader(conf);
        bamKey = new CreateDuplicationKey(header);
        dupKey = new DuplicationKeyWritable();
    }

    @Override
    public void map(LongWritable key, SamRecordWritable value, Context context) throws IOException, InterruptedException {
        bamKey.getKey(value.get(), dupKey);
        context.write(dupKey, value);
    }

}
