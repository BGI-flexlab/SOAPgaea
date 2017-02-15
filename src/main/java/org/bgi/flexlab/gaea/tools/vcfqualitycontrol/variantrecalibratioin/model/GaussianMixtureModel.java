package org.bgi.flexlab.gaea.tools.vcfqualitycontrol.variantrecalibratioin.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.bgi.flexlab.gaea.tools.vcfqualitycontrol.variantrecalibratioin.traindata.VariantDatum;
import org.bgi.flexlab.gaea.util.MathUtils;
import org.bgi.flexlab.gaea.util.RandomUtils;

import Jama.Matrix;
import cern.jet.random.Normal;

public class GaussianMixtureModel {
	private final ArrayList<MultipleVariateGaussian> gaussians;
    private final double shrinkage;
    private final double dirichletParameter;
    private final double priorCounts;
    private final double[] empiricalMu;
    private final Matrix empiricalSigma;
    public boolean isModelReadyForEvaluation;
    public boolean failedToConverge = false;

    public GaussianMixtureModel( final int numGaussians, final int numAnnotations,
                                 final double shrinkage, final double dirichletParameter, final double priorCounts ) {

        gaussians = new ArrayList<MultipleVariateGaussian>( numGaussians );
        for( int iii = 0; iii < numGaussians; iii++ ) {
            final MultipleVariateGaussian gaussian = new MultipleVariateGaussian( numAnnotations );
            gaussians.add( gaussian );
        }
        this.shrinkage = shrinkage;
        this.dirichletParameter = dirichletParameter;
        this.priorCounts = priorCounts;
        empiricalMu = new double[numAnnotations];
        empiricalSigma = new Matrix(numAnnotations, numAnnotations);
        isModelReadyForEvaluation = false;
        Arrays.fill(empiricalMu, 0.0);
        empiricalSigma.setMatrix(0, empiricalMu.length - 1, 0, empiricalMu.length - 1, Matrix.identity(empiricalMu.length, empiricalMu.length).times(200.0).inverse());
    }

    public void initializeRandomModel( final List<VariantDatum> data, final int numKMeansIterations ) {

        // initialize random Gaussian means // BUGBUG: this is broken up this way to match the order of calls to rand.nextDouble() in the old code
        for( final MultipleVariateGaussian gaussian : gaussians ) {
            gaussian.initializeRandomMu( RandomUtils.getRandomGenerator() );
        }

        // initialize means using K-means algorithm
        initializeMeansUsingKMeans( data, numKMeansIterations );

        // initialize uniform mixture coefficients, random covariance matrices, and initial hyperparameters
        for( final MultipleVariateGaussian gaussian : gaussians ) {
            gaussian.pMixtureLog10 = Math.log10( 1.0 / ((double) gaussians.size()) );
            gaussian.sumProb = 1.0 / ((double) gaussians.size());
            gaussian.initializeRandomSigma( RandomUtils.getRandomGenerator() );
            gaussian.hyperParameter_a = priorCounts;
            gaussian.hyperParameter_b = shrinkage;
            gaussian.hyperParameter_lambda = dirichletParameter;
        }
    }

    private void initializeMeansUsingKMeans( final List<VariantDatum> data, final int numIterations ) {

        int ttt = 0;
        while( ttt++ < numIterations ) {
            // E step: assign each variant to the nearest cluster
            for( final VariantDatum datum : data ) {
                double minDistance = Double.MAX_VALUE;
                MultipleVariateGaussian minGaussian = null;
                datum.assignment = minGaussian;
                for( final MultipleVariateGaussian gaussian : gaussians ) {
                    final double dist = gaussian.calculateDistanceFromMeanSquared( datum );
                    if( dist < minDistance ) {
                        minDistance = dist;
                        minGaussian = gaussian;
                    }
                }
                datum.assignment = minGaussian;
            }

            // M step: update gaussian means based on assigned variants
            for( final MultipleVariateGaussian gaussian : gaussians ) {
                gaussian.zeroOutMu();
                int numAssigned = 0;

                for( final VariantDatum datum : data ) {
                    if( datum.assignment.equals(gaussian) ) {
                        numAssigned++;
                        gaussian.incrementMu( datum );
                    }
                }
                if( numAssigned != 0 ) {
                    gaussian.divideEqualsMu( ((double) numAssigned) );
                } else {
                    gaussian.initializeRandomMu( RandomUtils.getRandomGenerator() );
                }
            }
        }
    }

    public void expectationStep( final List<VariantDatum> data ) {

        for( final MultipleVariateGaussian gaussian : gaussians ) {
            gaussian.precomputeDenominatorForVariationalBayes( getSumHyperParameterLambda() );
        }

        for( final VariantDatum datum : data ) {
            final double[] pVarInGaussianLog10 = new double[gaussians.size()];
            int gaussianIndex = 0;
            for( final MultipleVariateGaussian gaussian : gaussians ) {
                final double pVarLog10 = gaussian.evaluateDatumLog10( datum );
                pVarInGaussianLog10[gaussianIndex++] = pVarLog10;
            }
            final double[] pVarInGaussianNormalized = MathUtils.normalizeFromLog10( pVarInGaussianLog10, false );
            gaussianIndex = 0;
            for( final MultipleVariateGaussian gaussian : gaussians ) {
                gaussian.assignPVarInGaussian( pVarInGaussianNormalized[gaussianIndex++] );
            }
        }
    }

    public void maximizationStep( final List<VariantDatum> data ) {
        for( final MultipleVariateGaussian gaussian : gaussians ) {
            gaussian.maximizeGaussian( data, empiricalMu, empiricalSigma, shrinkage, dirichletParameter, priorCounts);
        }
    }

    private double getSumHyperParameterLambda() {
        double sum = 0.0;
        for( final MultipleVariateGaussian gaussian : gaussians ) {
            sum += gaussian.hyperParameter_lambda;
        }
        return sum;
    }

    public void evaluateFinalModelParameters( final List<VariantDatum> data ) {
        for( final MultipleVariateGaussian gaussian : gaussians ) {
            gaussian.evaluateFinalModelParameters(data);
        }
        normalizePMixtureLog10();
    }

    public double normalizePMixtureLog10() {
        double sumDiff = 0.0;
        double sumPK = 0.0;
        for( final MultipleVariateGaussian gaussian : gaussians ) {
            sumPK += gaussian.sumProb;
        }

        int gaussianIndex = 0;
        double[] pGaussianLog10 = new double[gaussians.size()];
        for( final MultipleVariateGaussian gaussian : gaussians ) {
            pGaussianLog10[gaussianIndex++] = Math.log10( gaussian.sumProb / sumPK );
        }
        pGaussianLog10 = MathUtils.normalizeFromLog10( pGaussianLog10, true );

        gaussianIndex = 0;
        for( final MultipleVariateGaussian gaussian : gaussians ) {
            sumDiff += Math.abs( pGaussianLog10[gaussianIndex] - gaussian.pMixtureLog10 );
            gaussian.pMixtureLog10 = pGaussianLog10[gaussianIndex++];
        }
        return sumDiff;
    }

    public void precomputeDenominatorForEvaluation() {
        for( final MultipleVariateGaussian gaussian : gaussians ) {
            gaussian.precomputeDenominatorForEvaluation();
        }

        isModelReadyForEvaluation = true;
    }

    public double evaluateDatum( final VariantDatum datum ) {
        for( final boolean isNull : datum.isNull ) {
            if( isNull ) { return evaluateDatumMarginalized( datum ); }
        }
        // Fill an array with the log10 probability coming from each Gaussian and then use MathUtils to sum them up correctly
        final double[] pVarInGaussianLog10 = new double[gaussians.size()];
        int gaussianIndex = 0;
        for( final MultipleVariateGaussian gaussian : gaussians ) {
            pVarInGaussianLog10[gaussianIndex++] = gaussian.pMixtureLog10 + gaussian.evaluateDatumLog10( datum );
        }
        return MathUtils.log10sumLog10(pVarInGaussianLog10); // Sum(pi_k * p(v|n,k))
    }

    // Used only to decide which covariate dimension is most divergent in order to report in the culprit info field annotation
    public Double evaluateDatumInOneDimension( final VariantDatum datum, final int iii ) {
        if(datum.isNull[iii]) { return null; }

        final Normal normal = new Normal(0.0, 1.0, null);
        final double[] pVarInGaussianLog10 = new double[gaussians.size()];
        int gaussianIndex = 0;
        for( final MultipleVariateGaussian gaussian : gaussians ) {
            normal.setState( gaussian.mu[iii], gaussian.sigma.get(iii, iii) );
            pVarInGaussianLog10[gaussianIndex++] = gaussian.pMixtureLog10 + Math.log10( normal.pdf( datum.annotations[iii] ) );
        }
        return MathUtils.log10sumLog10(pVarInGaussianLog10); // Sum(pi_k * p(v|n,k))
    }

    public double evaluateDatumMarginalized( final VariantDatum datum ) {
        int numRandomDraws = 0;
        double sumPVarInGaussian = 0.0;
        final int numIterPerMissingAnnotation = 10; // Trade off here between speed of computation and accuracy of the marginalization
        final double[] pVarInGaussianLog10 = new double[gaussians.size()];
        // for each dimension
        for( int iii = 0; iii < datum.annotations.length; iii++ ) {
            // if it is missing marginalize over the missing dimension by drawing X random values for the missing annotation and averaging the lod
            if( datum.isNull[iii] ) {
                for( int ttt = 0; ttt < numIterPerMissingAnnotation; ttt++ ) {
                    datum.annotations[iii] = RandomUtils.getRandomGenerator().nextGaussian(); // draw a random sample from the standard normal distribution

                    // evaluate this random data point
                    int gaussianIndex = 0;
                    for( final MultipleVariateGaussian gaussian : gaussians ) {
                        pVarInGaussianLog10[gaussianIndex++] = gaussian.pMixtureLog10 + gaussian.evaluateDatumLog10( datum );
                    }

                    // add this sample's probability to the pile in order to take an average in the end
                    sumPVarInGaussian += Math.pow(10.0, MathUtils.log10sumLog10(pVarInGaussianLog10)); // p = 10 ^ Sum(pi_k * p(v|n,k))
                    numRandomDraws++;
                }
            }
        }
        return Math.log10( sumPVarInGaussian / ((double) numRandomDraws) );
    }
}
