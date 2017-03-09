package org.bgi.flexlab.gaea.framework.tools.mapreduce;

import htsjdk.samtools.SAMRecord;
import org.bgi.flexlab.gaea.data.mapreduce.writable.AlignmentBasicWritable;
import org.bgi.flexlab.gaea.data.structure.alignment.AlignmentsBasic;

/**
 * Created by zhangyong on 2017/3/8.
 */
public class WindowsBasedAlignmentMapper extends WindowsBasedMapper<AlignmentBasicWritable>{
    @Override
    void setOutputValue(SAMRecord samRecord) {
        AlignmentsBasic alignmentsBasic = new AlignmentsBasic();
        alignmentsBasic.parseSAM(samRecord);
        outputValue.setAlignment(alignmentsBasic);
    }

    @Override
    void initOutputVaule() {
        outputValue = new AlignmentBasicWritable();
    }
}
