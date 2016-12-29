package org.bgi.flexlab.gaea.tools.variantrecalibratioin.traindata;

import java.io.Serializable;
import java.util.Comparator;

import org.bgi.flexlab.gaea.data.structure.location.GenomeLocation;
import org.bgi.flexlab.gaea.tools.variantrecalibratioin.model.MultipleVariateGaussian;

public class VariantDatum {
	public double[] annotations;
    public boolean[] isNull;
    public boolean isKnown;
    public double lod;
    public boolean atTruthSite;
    public boolean atTrainingSite;
    public boolean atAntiTrainingSite;
    public boolean isTransition;
    public boolean isSNP;
    public boolean failingSTDThreshold;
    public double originalQual;
    public double prior;
    public int consensusCount;
    public GenomeLocation loc;
    public int worstAnnotation;
    public MultipleVariateGaussian assignment; // used in K-means implementation 

    public static class VariantDatumLODComparator implements Comparator<VariantDatum>, Serializable {
        @Override
        public int compare(final VariantDatum datum1, final VariantDatum datum2) {
            return Double.compare(datum1.lod, datum2.lod);
        }
    }
}
