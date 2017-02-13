package org.bgi.flexlab.gaea.data.structure.pileup;

import htsjdk.samtools.SAMFileHeader;
import org.bgi.flexlab.gaea.data.mapreduce.writable.AlignmentBasicWritable;
import org.bgi.flexlab.gaea.data.structure.alignment.AlignmentsBasic;
import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;

import java.util.Iterator;
/**
 * Created by zhangyong on 2016/12/23.
 */
public class ReadsPool implements Iterator {
    private boolean isSAM;

    private Iterator<GaeaSamRecord> samReads;

    private Iterator<AlignmentBasicWritable> alignments;

    public ReadsPool(Iterator<GaeaSamRecord> samReads, SAMFileHeader header) {
        this.samReads = samReads;
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
            alignment = new AlignmentsBasic();
            alignment.parseSAM(samReads.next());
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
