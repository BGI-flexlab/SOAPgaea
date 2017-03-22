package org.bgi.flexlab.gaea.framework.tools.mapreduce;

import htsjdk.samtools.SAMRecord;
import org.bgi.flexlab.gaea.data.mapreduce.writable.SamRecordWritable;

/**
 * Created by zhangyong on 2017/3/8.
 */
public class WindowsBasedSamRecordMapper extends WindowsBasedMapper<SamRecordWritable>{
    @Override
    void setOutputValue(SAMRecord samRecord) {
        outputValue.set(samRecord);
    }

    @Override
    void initOutputVaule() {
        outputValue = new SamRecordWritable();
    }
}
