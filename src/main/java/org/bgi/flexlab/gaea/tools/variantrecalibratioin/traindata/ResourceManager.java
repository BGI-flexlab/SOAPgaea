package org.bgi.flexlab.gaea.tools.variantrecalibratioin.traindata;

import java.util.ArrayList;
import java.util.List;

import org.bgi.flexlab.gaea.tools.mapreduce.variantrecalibratioin.VariantRecalibrationOptions;
import htsjdk.variant.variantcontext.VariantContext;

public class ResourceManager {
	private final double[] annotations;
    private final boolean[] isNull;
    public final List<String> annotationKeys;
    //protected final List<TrainingSet> trainingSets;
    protected final List<TrainData> trainingSets;
    VariantRecalibrationOptions options;
    
    public ResourceManager( VariantRecalibrationOptions options) {
        this.annotationKeys = new ArrayList<String>( options.getUseAnnotations() );
        annotations = new double[annotationKeys.size()];
        isNull = new boolean[annotationKeys.size()];
        trainingSets = new ArrayList<TrainData>();
        this.options = options;
    }

    public void addTrainingSet( final TrainData trainingSet ) {
        trainingSets.add( trainingSet );
    }

    public boolean checkHasTrainingSet() {
        for( final TrainData trainingSet : trainingSets ) {
            if( trainingSet.isTraining() ) { return true; }
        }
        return false;
    }

    public boolean checkHasTruthSet() {
        for( final TrainData trainingSet : trainingSets ) {
            if( trainingSet.isTruth() ) { return true; }
        }
        return false;
    }

    public boolean checkHasKnownSet() {
        for( final TrainData trainingSet : trainingSets ) {
            if( trainingSet.isKnown() ) { return true; }
        }
        return false;
    }
    
    public double[] getAnnotations() {
    	return annotations;
    }
    
    public boolean[] getIsNull() {
    	return isNull;
    }
    
    public List<TrainData> getTrainDataSet() {
    	return trainingSets;
    }

    public static boolean checkVariationClass( final VariantContext evalVC, final VariantContext trainVC ) {
        switch( trainVC.getType() ) {
            case SNP:
            case MNP:
                return checkVariationClass( evalVC, VariantRecalibrationOptions.Mode.SNP );
            case INDEL:
            case MIXED:
            case SYMBOLIC:
                return checkVariationClass( evalVC, VariantRecalibrationOptions.Mode.INDEL );
            default:
                return false;
        }
    }

    public static boolean checkVariationClass( final VariantContext evalVC, final VariantRecalibrationOptions.Mode mode ) {
        switch( mode ) {
            case SNP:
                return evalVC.isSNP() || evalVC.isMNP();
            case INDEL:
                return evalVC.isStructuralIndel() || evalVC.isIndel() || evalVC.isMixed() || evalVC.isSymbolic();
            case BOTH:
                return true;
            default:
                throw new RuntimeException( "Encountered unknown recal mode: " + mode );
        }
    }

}
