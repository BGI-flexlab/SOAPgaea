package org.bgi.flexlab.gaea.tools.genotyer.genotypeLikelihoodCalculator;

/**
 * Created by zhangyong on 2016/12/29.
 */
public class SampleGenotypeData extends GenotypeData{
    private  String name;
    private  int depth;

    public SampleGenotypeData() {
        super();
    }

    public SampleGenotypeData(String name, int depth) {
        super();
        this.name = name;
        this.depth = depth;
    }

    public String getName() {
        return name;
    }

    public int getDepth() {
        return depth;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setDepth(int depth) {
        this.depth = depth;
    }
}
