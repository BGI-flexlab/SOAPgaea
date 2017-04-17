package org.bgi.flexlab.gaea.tools.mapreduce.markduplicate;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.bgi.flexlab.gaea.data.mapreduce.input.header.SamHdfsFileHeader;
import org.bgi.flexlab.gaea.data.mapreduce.writable.DuplicationKeyWritable;
import org.bgi.flexlab.gaea.data.mapreduce.writable.SamRecordWritable;
import org.bgi.flexlab.gaea.tools.markduplicate.MarkDuplicatesFunc;

import java.io.IOException;
import java.util.ArrayList;


/**
 * Created by huangzhibo on 2017/4/14.
 */
public class MarkDuplicateReducer extends Reducer<DuplicationKeyWritable, SamRecordWritable, NullWritable, SamRecordWritable>{
    MarkDuplicatesFunc mark = new MarkDuplicatesFunc();
    private SAMFileHeader samHeader;

    @Override
    public void setup(Context context){
        Configuration conf = context.getConfiguration();
        samHeader = SamHdfsFileHeader.getHeader(conf);
    }

    public void reduce(DuplicationKeyWritable key, Iterable<SamRecordWritable> values, Context context) throws IOException, InterruptedException {
        //deal unmapped reads
        if(key.getChrIndex() == -1) {
            for(SamRecordWritable s : values) {
                SAMRecord sam = s.get();
                sam.setHeader(samHeader);
                SamRecordWritable w = new SamRecordWritable();
                w.set(sam);
                context.write(NullWritable.get(), w);
            }
            return;
        }

        //collect reads cluster and mark duplicate
        ArrayList<SAMRecord> sams = new ArrayList<>();
        int n = 0;
        for(SamRecordWritable s : values) {
            SAMRecord sam=s.get();
            sam.setHeader(samHeader);
            if (n>5000) {
                sam.setDuplicateReadFlag(true);
                SamRecordWritable w = new SamRecordWritable();
                w.set(sam);
                context.write(NullWritable.get(), w);
            }else {
                sams.add(sam);
            }
            n++;
        }
        if (n>5000) {
            System.err.println("Dup > 5000. Pos:" + key.getChrIndex() + ":"+key.getPosition());
        }

        if(sams.size() > 1)
            mark.markDup(sams);
        for(SAMRecord sam : sams) {
            SamRecordWritable w = new SamRecordWritable();
            w.set(sam);
            context.write(NullWritable.get(), w);
        }
    }
}
