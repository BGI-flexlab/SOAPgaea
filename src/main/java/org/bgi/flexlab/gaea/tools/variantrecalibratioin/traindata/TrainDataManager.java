package org.bgi.flexlab.gaea.tools.variantrecalibratioin.traindata;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.bgi.flexlab.gaea.tools.mapreduce.variantrecalibratioin.VariantRecalibrationOptions;
import org.bgi.flexlab.gaea.tools.variantrecalibratioin.model.VariantDataManager;
import org.bgi.flexlab.gaea.util.MathUtils;
import org.bgi.flexlab.gaea.util.RandomUtils;

import htsjdk.variant.variantcontext.VariantContext;

public class TrainDataManager {
	private final double[] annotations;
    private final boolean[] isNull;
    public final List<String> annotationKeys;
    protected final static Logger logger = Logger.getLogger(VariantDataManager.class);
    //protected final List<TrainingSet> trainingSets;
    protected final List<TrainData> trainingSets;
    VariantRecalibrationOptions options;
    
    public TrainDataManager( final List<String> annotationKeys, VariantRecalibrationOptions options) {
        this.annotationKeys = new ArrayList<String>( annotationKeys );
        annotations = new double[annotationKeys.size()];
        isNull = new boolean[annotationKeys.size()];
        trainingSets = new ArrayList<TrainData>();
        this.options = options;
    }
    
    public void decodeAnnotations( final VariantContext vc, final boolean jitter ) {
        int iii = 0;
        for( final String key : annotationKeys ) {
            isNull[iii] = false;
            annotations[iii] = decodeAnnotation( key, vc, jitter );
            if( Double.isNaN(annotations[iii]) ) { isNull[iii] = true; }
            iii++;
        }
    }

    private static double decodeAnnotation( final String annotationKey, final VariantContext vc, final boolean jitter ) {
        double value;

        try {
            value = vc.getAttributeAsDouble( annotationKey, Double.NaN );
            if( Double.isInfinite(value) ) { value = Double.NaN; }
            if( jitter && annotationKey.equalsIgnoreCase("HRUN") ) { // Integer valued annotations must be jittered a bit to work in this GMM
                  value += -0.25 + 0.5 * RandomUtils.getRandomGenerator().nextDouble();
            }

            if( jitter && annotationKey.equalsIgnoreCase("HaplotypeScore") && MathUtils.compareDoubles(value, 0.0, 0.0001) == 0 ) { value = -0.2 + 0.4*RandomUtils.getRandomGenerator().nextDouble(); }
            if( jitter && annotationKey.equalsIgnoreCase("FS") && MathUtils.compareDoubles(value, 0.0, 0.001) == 0 ) { value = -0.2 + 0.4*RandomUtils.getRandomGenerator().nextDouble(); }
        } catch( Exception e ) {
            value = Double.NaN; // The VQSR works with missing data by marginalizing over the missing dimension when evaluating the Gaussian mixture model
        }

        return value;
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
