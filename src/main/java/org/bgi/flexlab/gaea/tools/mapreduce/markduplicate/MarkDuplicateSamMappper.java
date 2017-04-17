package org.bgi.flexlab.gaea.tools.mapreduce.markduplicate;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMLineParser;
import htsjdk.samtools.SAMRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.bgi.flexlab.gaea.data.mapreduce.input.header.SamHdfsFileHeader;
import org.bgi.flexlab.gaea.data.mapreduce.writable.CreateDuplicationKey;
import org.bgi.flexlab.gaea.data.mapreduce.writable.DuplicationKeyWritable;
import org.bgi.flexlab.gaea.data.mapreduce.writable.SamRecordWritable;

import java.io.IOException;


/**
 * Created by huangzhibo on 2017/4/14.
 */
public class MarkDuplicateSamMappper extends Mapper<LongWritable, SamRecordWritable, DuplicationKeyWritable, SamRecordWritable> {
    CreateDuplicationKey bamKey;
    private SAMFileHeader header;
    DuplicationKeyWritable dupKey;

    @Override
    public void setup(Context context) {
        Configuration conf = context.getConfiguration();
        header = SamHdfsFileHeader.getHeader(conf);
        bamKey = new CreateDuplicationKey(header);
        dupKey = new DuplicationKeyWritable();
    }

    @Override
    public void map(LongWritable key, SamRecordWritable value, Context context) throws IOException, InterruptedException {
        String readLine=value.toString();
        if (readLine.startsWith("@")) {
            return;
        }

        SAMLineParser parser = new SAMLineParser(header);
        SAMRecord sam = parser.parseLine(readLine);
        SamRecordWritable w = new SamRecordWritable();
        w.set(sam);
        bamKey.getKey(sam, dupKey);
        context.write(dupKey, w);
    }

}