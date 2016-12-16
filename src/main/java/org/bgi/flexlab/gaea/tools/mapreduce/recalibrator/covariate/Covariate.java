package org.bgi.flexlab.gaea.tools.mapreduce.recalibrator.covariate;

import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.tools.mapreduce.recalibrator.BaseRecalibrationOptions;
import org.bgi.flexlab.gaea.tools.mapreduce.recalibrator.ReadCovariates;

public interface Covariate {
	/**
     * Initialize any member variables
     */
    public void initialize(final BaseRecalibrationOptions option);

    /**
     * Calculates covariate values for all positions in the read.
     */
    public void recordValues(final GaeaSamRecord read, final ReadCovariates values);

    /**
     * Used to get the covariate's value from input
     */
    public Object getValue(final String str);

    /**
     * Converts the internal representation of the key to String format for file output.
     */
    public String formatKey(final int key);

    /**
     * Converts an Object key into a long key using only the lowest numberOfBits() bits
     *
     * Only necessary for on-the-fly recalibration when you have the object, but need to store it in memory in long format. For counting covariates
     * the getValues method already returns all values in long format.
     */
    public int keyFromValue(final Object value);

    /**
     * Returns the maximum value possible for any key representing this covariate
     */
    public int maximumKeyValue();
}
