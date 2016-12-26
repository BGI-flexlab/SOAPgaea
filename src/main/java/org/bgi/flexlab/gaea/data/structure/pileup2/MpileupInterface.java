package org.bgi.flexlab.gaea.data.structure.pileup2;


import java.util.Map;

/**
 * Created by zhangyong on 2016/12/26.
 */
public interface MpileupInterface<T extends PileupInterface<PileupReadInfo>>{

    boolean allEmpty();

    int forwardPosition(int minPosition, int size);

    void syn(int minPosition,Map<String, T> posPlps);

    int addReads(int minPosition);

    Map<String, T> getNextPosPileup();
}
