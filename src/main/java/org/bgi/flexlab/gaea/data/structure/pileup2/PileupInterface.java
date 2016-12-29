package org.bgi.flexlab.gaea.data.structure.pileup2;

import org.bgi.flexlab.gaea.data.structure.alignment.AlignmentsBasic;

import java.util.ArrayList;

/**
 * Created by zhangyong on 2016/12/26.
 */
public interface PileupInterface<T extends PileupReadInfo> {
    void calculateBaseInfo();

    void remove();

    void forwardPosition(int size);

    boolean isEmpty();

    void addReads(AlignmentsBasic read);

    ArrayList<T> getFinalPileup();
}
