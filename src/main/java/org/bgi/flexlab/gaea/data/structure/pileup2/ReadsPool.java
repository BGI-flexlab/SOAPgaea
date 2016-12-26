package org.bgi.flexlab.gaea.data.structure.pileup2;

import htsjdk.samtools.SAMFileHeader;
import org.bgi.flexlab.gaea.data.mapreduce.writable.AlignmentBasicWritable;
import org.bgi.flexlab.gaea.data.mapreduce.writable.SamRecordWritable;
import org.bgi.flexlab.gaea.data.structure.alignment.AlignmentsBasic;
import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;

import java.util.Iterator;
/**
 * Created by zhangyong on 2016/12/23.
 */
public class ReadsPool implements Iterator {
    private boolean isSAM;

    private Iterator<SamRecordWritable> samReads;

    private SAMFileHeader header;

    private Iterator<AlignmentBasicWritable> alignments;

    public ReadsPool(Iterator<SamRecordWritable> samReads, SAMFileHeader header) {
        this.samReads = samReads;
        this.header = header;
        isSAM = true;
    }

    public ReadsPool(Iterator<AlignmentBasicWritable> alignments) {
        this.alignments = alignments;
        isSAM = false;
    }

    @Override
    public AlignmentsBasic next() {
        AlignmentsBasic alignment;
        if(isSAM) {
            GaeaSamRecord samRecord = new GaeaSamRecord(header, samReads.next().get());
            alignment = new AlignmentsBasic();
            alignment.parseSAM(samRecord);
        } else {
            alignment = alignments.next().getAlignment();
        }

        return alignment;
    }

    @Override
    public boolean hasNext() {
        if (isSAM) {
            return samReads.hasNext();
        } else {
            return alignments.hasNext();
        }
    }
}
