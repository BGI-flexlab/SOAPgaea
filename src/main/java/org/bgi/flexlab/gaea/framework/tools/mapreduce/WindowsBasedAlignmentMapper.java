package org.bgi.flexlab.gaea.framework.tools.mapreduce;

import htsjdk.samtools.SAMRecord;
import org.apache.hadoop.mapreduce.Mapper;
import org.bgi.flexlab.gaea.data.mapreduce.writable.AlignmentBasicWritable;
import org.bgi.flexlab.gaea.data.structure.alignment.AlignmentsBasic;

/**
 * Created by zhangyong on 2017/3/8.
 */
public class WindowsBasedAlignmentMapper extends WindowsBasedMapper<AlignmentBasicWritable>{
    @Override
    void otherSetup(Context context) {
        AlignmentsBasic.initIdSampleHash(header.getReadGroups());
    }

    @Override
    void setOutputValue(SAMRecord samRecord) {
        AlignmentsBasic alignmentsBasic = new AlignmentsBasic();
        samRecord.setHeader(header);
        alignmentsBasic.parseSAM(samRecord);
        alignmentsBasic.setSampleIndex(samRecord.getReadGroup().getSample());
        outputValue.setAlignment(alignmentsBasic);
    }

    @Override
    void initOutputVaule() {
        outputValue = new AlignmentBasicWritable();
    }
}
