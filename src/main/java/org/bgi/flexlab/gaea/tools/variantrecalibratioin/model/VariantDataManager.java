package org.bgi.flexlab.gaea.tools.variantrecalibratioin.model;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.bgi.flexlab.gaea.data.exception.UserException;
import org.bgi.flexlab.gaea.data.structure.header.VCFConstants;
import org.bgi.flexlab.gaea.data.structure.location.GenomeLocation;
import org.bgi.flexlab.gaea.tools.mapreduce.variantrecalibratioin.VariantRecalibration;
import org.bgi.flexlab.gaea.tools.mapreduce.variantrecalibratioin.VariantRecalibrationOptions;
import org.bgi.flexlab.gaea.tools.variantrecalibratioin.traindata.TrainData;
import org.bgi.flexlab.gaea.tools.variantrecalibratioin.traindata.VariantDatum;
import org.bgi.flexlab.gaea.tools.variantrecalibratioin.traindata.VariantDatumMessenger;
import org.bgi.flexlab.gaea.util.ExpandingArrayList;
import org.bgi.flexlab.gaea.util.MathUtils;

import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;
import htsjdk.variant.variantcontext.writer.VariantContextWriter;

public class VariantDataManager {
	private ExpandingArrayList<VariantDatum> data;
    private final double[] meanVector;
    private final double[] varianceVector; // this is really the standard deviation
    private final double[] annotations;
    private final boolean[] isNull;
    public final List<String> annotationKeys;
    protected final static Logger logger = Logger.getLogger(VariantDataManager.class);
    //protected final List<TrainingSet> trainingSets;
    protected final List<TrainData> trainingSets;
    VariantRecalibrationOptions parameter;

    public VariantDataManager( final List<String> annotationKeys, VariantRecalibrationOptions parameter) {
        this.data = new ExpandingArrayList<VariantDatum>();
        this.annotationKeys = new ArrayList<String>( annotationKeys );
        annotations = new double[annotationKeys.size()];
        isNull = new boolean[annotationKeys.size()];
        meanVector = new double[this.annotationKeys.size()];
        varianceVector = new double[this.annotationKeys.size()];
        trainingSets = new ArrayList<TrainData>();
        this.parameter = parameter;
    }

    public void setData( final ExpandingArrayList<VariantDatum> data ) {
        this.data = data;
    }
    
    public void addData(VariantDatumMessenger messenger) {
    	VariantDatum variantDatum = new VariantDatum();
    	variantDatum.annotations = Arrays.copyOf(messenger.getAnnotations(), messenger.getAnnotations().length);
    	variantDatum.isNull = Arrays.copyOf(messenger.getIsNull(), messenger.getIsNull().length);
    	variantDatum.isKnown = messenger.checkFlag(VariantDatumMessenger.isKnown);
    	variantDatum.lod = messenger.getLod();
    	variantDatum.atTruthSite = messenger.checkFlag(VariantDatumMessenger.atTruthSite);
    	variantDatum.atTrainingSite = messenger.checkFlag(VariantDatumMessenger.atTrainingSite);
    	variantDatum.atAntiTrainingSite = messenger.checkFlag(VariantDatumMessenger.atAntiTrainingSite);
    	variantDatum.isTransition = messenger.checkFlag(VariantDatumMessenger.isTransition);
    	variantDatum.isSNP = messenger.checkFlag(VariantDatumMessenger.isSNP);
    	variantDatum.originalQual = messenger.getOriginalQual();
    	variantDatum.prior = messenger.getPrior();
    	variantDatum.consensusCount = messenger.getConsensusCount();
    	variantDatum.loc = messenger.getLoc();
    	variantDatum.worstAnnotation = messenger.getWorstAnnotation();
    	
    	data.add(variantDatum);
    }

    public ExpandingArrayList<VariantDatum> getData() {
        return data;
    }

    public void normalizeData() {
        boolean foundZeroVarianceAnnotation = false;
        for( int iii = 0; iii < meanVector.length; iii++ ) {
            final double theMean = mean(iii);
            final double theSTD = standardDeviation(theMean, iii);
            logger.info( annotationKeys.get(iii) + String.format(": \t mean = %.2f\t standard deviation = %.2f", theMean, theSTD) );
            if( Double.isNaN(theMean) ) {
                throw new UserException.BadInput("Values for " + annotationKeys.get(iii) + " annotation not detected for ANY training variant in the input callset. VariantAnnotator may be used to add these annotations.");
            }

            foundZeroVarianceAnnotation = foundZeroVarianceAnnotation || (theSTD < 1E-6);
            meanVector[iii] = theMean;
            varianceVector[iii] = theSTD;
            for( final VariantDatum datum : data ) {
                // Transform each data point via: (x - mean) / standard deviation
                datum.annotations[iii] = ( datum.isNull[iii] ? GaeaRandom.getRandomGenerator().nextGaussian() : ( datum.annotations[iii] - theMean ) / theSTD );
            }
        }
        if( foundZeroVarianceAnnotation ) {
            throw new UserException.BadInput( "Found annotations with zero variance. They must be excluded before proceeding." );
        }

        // trim data by standard deviation threshold and mark failing data for exclusion later
        for( final VariantDatum datum : data ) {
            boolean remove = false;
            for( final double val : datum.annotations ) {
                remove = remove || (Math.abs(val) > parameter.getStdThreshold());
            }
            datum.failingSTDThreshold = remove;
        }
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

    public ExpandingArrayList<VariantDatum> getTrainingData() {
        final ExpandingArrayList<VariantDatum> trainingData = new ExpandingArrayList<VariantDatum>();
        for( final VariantDatum datum : data ) {
            if( datum.atTrainingSite && !datum.failingSTDThreshold && datum.originalQual > parameter.getQualThreshold() ) {
                trainingData.add( datum );
            }
        }
        logger.info( "Training with " + trainingData.size() + " variants after standard deviation thresholding." );
        if( trainingData.size() < parameter.getMinNumBadVariants() ) {
            logger.warn( "WARNING: Training with very few variant sites! Please check the model reporting PDF to ensure the quality of the model is reliable." );
        }
        return trainingData;
    }

    public ExpandingArrayList<VariantDatum> selectWorstVariants( double bottomPercentage, final int minimumNumber ) {
        // The return value is the list of training variants
        final ExpandingArrayList<VariantDatum> trainingData = new ExpandingArrayList<VariantDatum>();

        // First add to the training list all sites overlapping any bad sites training tracks
        for( final VariantDatum datum : data ) {
            if( datum.atAntiTrainingSite && !datum.failingSTDThreshold && !Double.isInfinite(datum.lod) ) {
                trainingData.add( datum );
            }
        }
        final int numBadSitesAdded = trainingData.size();
        logger.info( "Found " + numBadSitesAdded + " variants overlapping bad sites training tracks." );

        // Next sort the variants by the LOD coming from the positive model and add to the list the bottom X percent of variants
        Collections.sort( data, new VariantDatum.VariantDatumLODComparator() );
        final int numToAdd = Math.max( minimumNumber - trainingData.size(), Math.round((float)bottomPercentage * data.size()) );
        if( numToAdd > data.size() ) {
            throw new UserException.BadInput( "Error during negative model training. Minimum number of variants to use in training is larger than the whole call set. One can attempt to lower the --minNumBadVariants arugment but this is unsafe." );
        } else if( numToAdd == minimumNumber - trainingData.size() ) {
            logger.warn( "WARNING: Training with very few variant sites! Please check the model reporting PDF to ensure the quality of the model is reliable." );
            bottomPercentage = ((float) numToAdd) / ((float) data.size());
        }
        int index = 0, numAdded = 0;
        while( numAdded < numToAdd && index < data.size() ) {
            final VariantDatum datum = data.get(index++);
            if( datum != null && !datum.atAntiTrainingSite && !datum.failingSTDThreshold && !Double.isInfinite(datum.lod) ) {
                datum.atAntiTrainingSite = true;
                trainingData.add( datum );
                numAdded++;
            }
        }
        logger.info( "Additionally training with worst " + String.format("%.3f", (float) bottomPercentage * 100.0f) + "% of passing data --> " + (trainingData.size() - numBadSitesAdded) + " variants with LOD <= " + String.format("%.4f", data.get(index).lod) + "." );
        return trainingData;
    }

    public ExpandingArrayList<VariantDatum> getRandomDataForPlotting( int numToAdd ) {
        numToAdd = Math.min(numToAdd, data.size());
        final ExpandingArrayList<VariantDatum> returnData = new ExpandingArrayList<VariantDatum>();
        for( int iii = 0; iii < numToAdd; iii++) {
            final VariantDatum datum = data.get(GaeaRandom.getRandomGenerator().nextInt(data.size()));
            if( !datum.failingSTDThreshold ) {
                returnData.add(datum);
            }
        }

        // Add an extra 5% of points from bad training set, since that set is small but interesting
        for( int iii = 0; iii < Math.floor(0.05*numToAdd); iii++) {
            final VariantDatum datum = data.get(GaeaRandom.getRandomGenerator().nextInt(data.size()));
            if( datum.atAntiTrainingSite && !datum.failingSTDThreshold ) { returnData.add(datum); }
            else { iii--; }
        }

        return returnData;
    }

    private double mean( final int index ) {
        double sum = 0.0;
        int numNonNull = 0;
        for( final VariantDatum datum : data ) {
            if( datum.atTrainingSite && !datum.isNull[index] ) { sum += datum.annotations[index]; numNonNull++; }
        }
        return sum / ((double) numNonNull);
    }

    private double standardDeviation( final double mean, final int index ) {
        double sum = 0.0;
        int numNonNull = 0;
        for( final VariantDatum datum : data ) {
            if( datum.atTrainingSite && !datum.isNull[index] ) { sum += ((datum.annotations[index] - mean)*(datum.annotations[index] - mean)); numNonNull++; }
        }
        return Math.sqrt( sum / ((double) numNonNull) );
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
                  value += -0.25 + 0.5 * GaeaRandom.getRandomGenerator().nextDouble();
            }

            if( jitter && annotationKey.equalsIgnoreCase("HaplotypeScore") && MathUtils.compareDoubles(value, 0.0, 0.0001) == 0 ) { value = -0.2 + 0.4*GaeaRandom.getRandomGenerator().nextDouble(); }
            if( jitter && annotationKey.equalsIgnoreCase("FS") && MathUtils.compareDoubles(value, 0.0, 0.001) == 0 ) { value = -0.2 + 0.4*GaeaRandom.getRandomGenerator().nextDouble(); }
        } catch( Exception e ) {
            value = Double.NaN; // The VQSR works with missing data by marginalizing over the missing dimension when evaluating the Gaussian mixture model
        }

        return value;
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

    public void writeOutRecalibrationTable( final VariantContextWriter recalWriter ) {
        // we need to sort in coordinate order in order to produce a valid VCF
        Collections.sort( data, new Comparator<VariantDatum>() {
            public int compare(VariantDatum vd1, VariantDatum vd2) {
                return vd1.loc.compareTo(vd2.loc);
            }} );

        // create dummy alleles to be used
        final List<Allele> alleles = new ArrayList<Allele>(2);
        alleles.add(Allele.create("N", true));
        alleles.add(Allele.create("<VQSR>", false));

        // to be used for the important INFO tags
        final HashMap<String, Object> attributes = new HashMap<String, Object>(3);

        for( final VariantDatum datum : data ) {
            attributes.put(VCFConstants.END_KEY, datum.loc.getStop());
            attributes.put(VariantRecalibration.VQS_LOD_KEY, String.format("%.4f", datum.lod));
            attributes.put(VariantRecalibration.CULPRIT_KEY, (datum.worstAnnotation != -1 ? annotationKeys.get(datum.worstAnnotation) : "NULL"));

            VariantContextBuilder builder = new VariantContextBuilder("VQSR", datum.loc.getContig(), datum.loc.getStart(), datum.loc.getStop(), alleles).attributes(attributes);
            recalWriter.add(builder.make());
        }
    }
  
}
