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

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(name);
        sb.append("\t");
        sb.append(depth);
        for (int i = 0; i < log10Likelihoods.length; i++) {
            sb.append("\t");
            sb.append(i);
            sb.append("\t");
            sb.append(log10Likelihoods[i]);
        }
        return sb.toString();
    }
}
