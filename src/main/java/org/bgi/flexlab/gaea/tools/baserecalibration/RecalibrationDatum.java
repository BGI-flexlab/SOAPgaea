package org.bgi.flexlab.gaea.tools.baserecalibration;

import java.util.Random;

import com.google.java.contract.Ensures;
import com.google.java.contract.Invariant;
import com.google.java.contract.Requires;


@Invariant({
        "estimatedQReported >= 0.0",
        "! Double.isNaN(estimatedQReported)",
        "! Double.isInfinite(estimatedQReported)",
        "empiricalQuality >= 0.0 || empiricalQuality == UNINITIALIZED",
        "! Double.isNaN(empiricalQuality)",
        "! Double.isInfinite(empiricalQuality)",
        "numObservations >= 0",
        "numMismatches >= 0",
        "numMismatches <= numObservations"
})
public class RecalibrationDatum {
    private static final double UNINITIALIZED = -1.0;

    /**
     * estimated reported quality score based on combined data's individual q-reporteds and number of observations
     */
    private double estimatedQReported;

    /**
     * the empirical quality for datums that have been collapsed together (by read group and reported quality, for example)
     */
    private double empiricalQuality;

    /**
     * number of bases seen in total
     */
    private long numObservations;

    /**
     * number of bases seen that didn't match the reference
     */
    private long numMismatches;

    /**
     * used when calculating empirical qualities to avoid division by zero
     */
    private static final int SMOOTHING_CONSTANT = 1;

    //---------------------------------------------------------------------------------------------------------------
    //
    // constructors
    //
    //---------------------------------------------------------------------------------------------------------------

    /**
     * Create a new RecalDatum with given observation and mismatch counts, and an reported quality
     *
     * @param _numObservations
     * @param _numMismatches
     * @param reportedQuality
     */
    public RecalibrationDatum(final long _numObservations, final long _numMismatches, final byte reportedQuality) {
        if ( _numObservations < 0 ) throw new IllegalArgumentException("numObservations < 0");
        if ( _numMismatches < 0 ) throw new IllegalArgumentException("numMismatches < 0");
        if ( reportedQuality < 0 ) throw new IllegalArgumentException("reportedQuality < 0");

        numObservations = _numObservations;
        numMismatches = _numMismatches;
        estimatedQReported = reportedQuality;
        empiricalQuality = UNINITIALIZED;
    }

   
    
    public RecalibrationDatum(final long _numObservations, final long _numMismatches, final double reportedQuality) {
        if ( _numObservations < 0 ) throw new IllegalArgumentException("numObservations < 0");
        if ( _numMismatches < 0 ) throw new IllegalArgumentException("numMismatches < 0");
        if ( reportedQuality < 0 ) throw new IllegalArgumentException("reportedQuality < 0");

        numObservations = _numObservations;
        numMismatches = _numMismatches;
        estimatedQReported = reportedQuality;
        empiricalQuality = UNINITIALIZED;
    }
    
    /**
     * Copy copy into this recal datum, overwriting all of this objects data
     * @param copy
     */
    public RecalibrationDatum(final RecalibrationDatum copy) {
        this.numObservations = copy.getNumObservations();
        this.numMismatches = copy.getNumMismatches();
        this.estimatedQReported = copy.estimatedQReported;
        this.empiricalQuality = copy.empiricalQuality;
    }

    /**
     * Add in all of the data from other into this object, updating the reported quality from the expected
     * error rate implied by the two reported qualities
     *
     * @param other
     */
    public synchronized void combine(final RecalibrationDatum other) {
        final double sumErrors = this.calcExpectedErrors() + other.calcExpectedErrors();
        increment(other.getNumObservations(), other.getNumMismatches());
        estimatedQReported = -10 * Math.log10(sumErrors / getNumObservations());
        empiricalQuality = UNINITIALIZED;
    }

    public synchronized void setEstimatedQReported(final double estimatedQReported) {
        if ( estimatedQReported < 0 ) throw new IllegalArgumentException("estimatedQReported < 0");
        if ( Double.isInfinite(estimatedQReported) ) throw new IllegalArgumentException("estimatedQReported is infinite");
        if ( Double.isNaN(estimatedQReported) ) throw new IllegalArgumentException("estimatedQReported is NaN");

        this.estimatedQReported = estimatedQReported;
    }

    public static RecalibrationDatum createRandomRecalDatum(int maxObservations, int maxErrors) {
        final Random random = new Random();
        final int nObservations = random.nextInt(maxObservations);
        final int nErrors = random.nextInt(maxErrors);
        final int qual = random.nextInt(QualityUtils.MAX_QUAL_SCORE);
        return new RecalibrationDatum(nObservations, nErrors, (byte)qual);
    }

    public final double getEstimatedQReported() {
        
    	return estimatedQReported;
    }
    public final byte getEstimatedQReportedAsByte() {
        return (byte)(int)(Math.round(getEstimatedQReported()));
    }

    //---------------------------------------------------------------------------------------------------------------
    //
    // Empirical quality score -- derived from the num mismatches and observations
    //
    //---------------------------------------------------------------------------------------------------------------

    /**
     * Returns the error rate (in real space) of this interval, or 0 if there are no obserations
     * @return the empirical error rate ~= N errors / N obs
     */
    @Ensures("result >= 0.0")
    public double getEmpiricalErrorRate() {
        if ( numObservations == 0 )
            return 0.0;
        else {
            // cache the value so we don't call log over and over again
            final double doubleMismatches = (double) (numMismatches + SMOOTHING_CONSTANT);
            // smoothing is one error and one non-error observation, for example
            final double doubleObservations = (double) (numObservations + SMOOTHING_CONSTANT + SMOOTHING_CONSTANT);
            return doubleMismatches / doubleObservations;
        }
    }

    public synchronized void setEmpiricalQuality(final double empiricalQuality) {
        if ( empiricalQuality < 0 ) throw new IllegalArgumentException("empiricalQuality < 0");
        if ( Double.isInfinite(empiricalQuality) ) throw new IllegalArgumentException("empiricalQuality is infinite");
        if ( Double.isNaN(empiricalQuality) ) throw new IllegalArgumentException("empiricalQuality is NaN");

        this.empiricalQuality = empiricalQuality;
    }

    public final double getEmpiricalQuality() {
        if (empiricalQuality == UNINITIALIZED)
            calcEmpiricalQuality();
        return empiricalQuality;
    }

    public final byte getEmpiricalQualityAsByte() {
        return (byte)(Math.round(getEmpiricalQuality()));
    }

        //---------------------------------------------------------------------------------------------------------------
    //
    // increment methods
    //
    //---------------------------------------------------------------------------------------------------------------

    @Override
    public String toString() {
        return String.format("%d\t%d\t%.4f", getNumObservations(), getNumMismatches(),estimatedQReported);
    }

    public String stringForCSV() {
        return String.format("%s,%d,%.2f", toString(), (byte) Math.floor(getEstimatedQReported()), getEmpiricalQuality() - getEstimatedQReported());
    }


    //---------------------------------------------------------------------------------------------------------------
    //
    // increment methods
    //
    //---------------------------------------------------------------------------------------------------------------

    public long getNumObservations() {
        return numObservations;
    }

    public synchronized void setNumObservations(final long numObservations) {
        if ( numObservations < 0 ) throw new IllegalArgumentException("numObservations < 0");
        this.numObservations = numObservations;
        empiricalQuality = UNINITIALIZED;
    }

    public long getNumMismatches() {
        return numMismatches;
    }

    @Requires({"numMismatches >= 0"})
    public synchronized void setNumMismatches(final long numMismatches) {
        if ( numMismatches < 0 ) throw new IllegalArgumentException("numMismatches < 0");
        this.numMismatches = numMismatches;
        empiricalQuality = UNINITIALIZED;
    }

    @Requires({"by >= 0"})
    public synchronized void incrementNumObservations(final long by) {
        numObservations += by;
        empiricalQuality = UNINITIALIZED;
    }

    @Requires({"by >= 0"})
    public synchronized void incrementNumMismatches(final long by) {
        numMismatches += by;
        empiricalQuality = UNINITIALIZED;
    }

    @Requires({"incObservations >= 0", "incMismatches >= 0"})
    @Ensures({"numObservations == old(numObservations) + incObservations", "numMismatches == old(numMismatches) + incMismatches"})
    public synchronized void increment(final long incObservations, final long incMismatches) {
        incrementNumObservations(incObservations);
        incrementNumMismatches(incMismatches);
    }

    @Ensures({"numObservations == old(numObservations) + 1", "numMismatches >= old(numMismatches)"})
    public synchronized void increment(final boolean isError) {
        incrementNumObservations(1);
        if ( isError )
            incrementNumMismatches(1);
    }

    public synchronized void increment(final RecalibrationDatum qualThisDatum) {
       if(qualThisDatum==null){
    	   System.out.println("the incremented qual datum is null");
    	   System.exit(-1);
       }
    	incrementNumObservations(qualThisDatum.numObservations);
        incrementNumMismatches(qualThisDatum.numMismatches);
    }

    // -------------------------------------------------------------------------------------
    //
    // Private implementation helper functions
    //
    // -------------------------------------------------------------------------------------

    /**
     * Calculate and cache the empirical quality score from mismatches and observations (expensive operation)
     */
    @Requires("empiricalQuality == UNINITIALIZED")
    @Ensures("empiricalQuality != UNINITIALIZED")
    private synchronized final void calcEmpiricalQuality() {
        final double empiricalQual = -10 * Math.log10(getEmpiricalErrorRate());
        empiricalQuality = Math.min(empiricalQual, (double) QualityUtils.MAX_RECALIBRATED_Q_SCORE);
    }

    /**
     * calculate the expected number of errors given the estimated Q reported and the number of observations
     * in this datum.
     *
     * @return a positive (potentially fractional) estimate of the number of errors
     */
    @Ensures("result >= 0.0")
    private double calcExpectedErrors() {
        return (double) getNumObservations() * QualityUtils.qualToErrorProb(estimatedQReported);
    }
}
