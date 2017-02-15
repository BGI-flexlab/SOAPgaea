package org.bgi.flexlab.gaea.tools.variantrecalibratioin;

import java.util.List;

import org.bgi.flexlab.gaea.tools.mapreduce.vcfqualitycontrol.VCFQualityControlOptions;
import org.bgi.flexlab.gaea.tools.variantrecalibratioin.model.GaussianMixtureModel;
import org.bgi.flexlab.gaea.tools.variantrecalibratioin.traindata.VariantDatum;
import org.bgi.flexlab.gaea.util.RandomUtils;

public class VariantRecalibrationEngine {

	public final static double MIN_ACCEPTABLE_LOD_SCORE = -20000.0;

    // the unified argument collection
    final private VCFQualityControlOptions options;

    private final static double MIN_PROB_CONVERGENCE = 2E-2;

    /////////////////////////////
    // Public Methods to interface with the Engine
    /////////////////////////////

    public VariantRecalibrationEngine( final VCFQualityControlOptions options ) {
        this.options = options;
    }

    public GaussianMixtureModel generateModel( final List<VariantDatum> data ) {
        final GaussianMixtureModel model = new GaussianMixtureModel( options.getMaxGaussians(), data.get(0).annotations.length, options.getShrinkage(), options.getDirichletParamenter(), options.getPriorCounts() );
        variationalBayesExpectationMaximization( model, data );
        return model;
    }

    public void evaluateData( final List<VariantDatum> data, final GaussianMixtureModel model, final boolean evaluateContrastively ) {
        if( !model.isModelReadyForEvaluation ) {
            try {
                model.precomputeDenominatorForEvaluation();
            } catch( Exception e ) {
                model.failedToConverge = true;
                return;
            }
        }
        
        for( final VariantDatum datum : data ) {
            final double thisLod = evaluateDatum( datum, model );
            if( Double.isNaN(thisLod) ) {
                model.failedToConverge = true;
                return;
            }

            datum.lod = ( evaluateContrastively ?
                            ( Double.isInfinite(datum.lod) ? // positive model said negative infinity
                                    ( MIN_ACCEPTABLE_LOD_SCORE + RandomUtils.getRandomGenerator().nextDouble() * MIN_ACCEPTABLE_LOD_SCORE ) // Negative infinity lod values are possible when covariates are extremely far away from their tight Gaussians
                                    : datum.prior + datum.lod - thisLod) // contrastive evaluation: (prior + positive model - negative model)
                            : thisLod ); // positive model only so set the lod and return
        }
    }

    public void calculateWorstPerformingAnnotation( final List<VariantDatum> data, final GaussianMixtureModel goodModel, final GaussianMixtureModel badModel ) {
        for( final VariantDatum datum : data ) {
            int worstAnnotation = -1;
            double minProb = Double.MAX_VALUE;
            for( int iii = 0; iii < datum.annotations.length; iii++ ) {
                final Double goodProbLog10 = goodModel.evaluateDatumInOneDimension(datum, iii);
                final Double badProbLog10 = badModel.evaluateDatumInOneDimension(datum, iii);
                if( goodProbLog10 != null && badProbLog10 != null ) {
                    final double prob = goodProbLog10 - badProbLog10;
                    if(prob < minProb) { minProb = prob; worstAnnotation = iii; }
                }
            }
            datum.worstAnnotation = worstAnnotation;
        }
    }


    /////////////////////////////
    // Private Methods used for generating a GaussianMixtureModel
    /////////////////////////////

    private void variationalBayesExpectationMaximization( final GaussianMixtureModel model, final List<VariantDatum> data ) {

        model.initializeRandomModel( data, options.getNumKMeansIterations() );

        // The VBEM loop
        model.normalizePMixtureLog10();
        model.expectationStep( data );
        double currentChangeInMixtureCoefficients;
        int iteration = 0;
        while( iteration < options.getMaxIterations() ) {
            iteration++;
            model.maximizationStep( data );
            currentChangeInMixtureCoefficients = model.normalizePMixtureLog10();
            model.expectationStep( data );
            if( iteration % 5 == 0 ) { // cut down on the number of output lines so that users can read the warning messages
            }
            if( iteration > 2 && currentChangeInMixtureCoefficients < MIN_PROB_CONVERGENCE ) {
                break;
            }
        }

        model.evaluateFinalModelParameters( data );
    }

    /////////////////////////////
    // Private Methods used for evaluating data given a GaussianMixtureModel
    /////////////////////////////

    private double evaluateDatum( final VariantDatum datum, final GaussianMixtureModel model ) {
        return model.evaluateDatum( datum );
    }
}
