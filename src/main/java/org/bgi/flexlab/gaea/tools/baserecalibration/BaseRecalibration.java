
package org.bgi.flexlab.gaea.tools.baserecalibration;

import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.exception.UserException;
import org.bgi.flexlab.gaea.tools.baserecalibration.covariates.Covariate;
import org.bgi.flexlab.gaea.util.EventType;
import org.bgi.flexlab.gaea.util.MathUtils;
import org.bgi.flexlab.gaea.util.NestedHashMap;
import org.bgi.flexlab.gaea.util.NestedIntegerArray;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMTag;
import htsjdk.samtools.SAMUtils;




public class BaseRecalibration {
    private final static int MAXIMUM_RECALIBRATED_READ_LENGTH = 5000;
    private final ReadCovariates readCovariates;

    private final QuantizationInformation quantizationInfo;                                                                    // histogram containing the map for qual quantization (calculated after recalibration is done)
    private final RecalibrationTables recalibrationTables;
    private final Covariate[] requestedCovariates;                                                                      // list of all covariates to be used in this calculation

    private final boolean disableIndelQuals;
    private final int preserveQLessThan;
    private final boolean emitOriginalQuals;

    private static final NestedHashMap[] qualityScoreByFullCovariateKey = new NestedHashMap[EventType.values().length]; // Caches the result of performSequentialQualityCalculation(..) for all sets of covariate values.
    static {
        for (int i = 0; i < EventType.values().length; i++)
            qualityScoreByFullCovariateKey[i] = new NestedHashMap();
    }

    /**
     * Constructor using a report file
     * 
     * @param RECAL_FILE         a report file containing the recalibration information
     * @param quantizationLevels number of bins to quantize the quality scores
     * @param disableIndelQuals  if true, do not emit base indel qualities
     * @param preserveQLessThan  preserve quality scores less than this value
     */
    public BaseRecalibration(final String RECAL_FILE,SAMFileHeader mFileHeader, final int quantizationLevels, final boolean disableIndelQuals, final int preserveQLessThan, final boolean emitOriginalQuals) {
        RecalibrationReport recalibrationReport = new RecalibrationReport(RECAL_FILE,mFileHeader);

        recalibrationTables = recalibrationReport.getRecalibrationTables();
        requestedCovariates = recalibrationReport.getRequestedCovariates();
        quantizationInfo = recalibrationReport.getQuantizationInfo();
        if (quantizationLevels == 0)                                                                                    // quantizationLevels == 0 means no quantization, preserve the quality scores
            quantizationInfo.noQuantization();
        else if (quantizationLevels > 0 && quantizationLevels != quantizationInfo.getQuantizationLevels())              // any other positive value means, we want a different quantization than the one pre-calculated in the recalibration report. Negative values mean the user did not provide a quantization argument, and just wnats to use what's in the report.
            quantizationInfo.quantizeQualityScores(quantizationLevels);

        readCovariates = new ReadCovariates(MAXIMUM_RECALIBRATED_READ_LENGTH, requestedCovariates.length);
        readCovariates.setLargeKeys(MAXIMUM_RECALIBRATED_READ_LENGTH, requestedCovariates.length);
        this.disableIndelQuals = disableIndelQuals;
        this.preserveQLessThan = preserveQLessThan;
        this.emitOriginalQuals = emitOriginalQuals;
    }

    /**
     * Recalibrates the base qualities of a read
     *
     * It updates the base qualities of the read with the new recalibrated qualities (for all event types)
     *
     * @param read the read to recalibrate
     */
    public void recalibrateRead(final GaeaSamRecord read) {
        if (emitOriginalQuals && read.getAttribute(SAMTag.OQ.name()) == null) { // Save the old qualities if the tag isn't already taken in the read
            try {
                read.setAttribute(SAMTag.OQ.name(), SAMUtils.phredToFastq(read.getBaseQualities()));
            } catch (IllegalArgumentException e) {
                throw new UserException.MalformedBAM(read, "illegal base quality encountered; " + e.getMessage());
            }
        }

        RecalibrationUtils.computeCovariates(read, requestedCovariates, readCovariates);                                  // compute all covariates for the read
        for (final EventType errorModel : EventType.values()) {                                                         // recalibrate all three quality strings
            if (disableIndelQuals && errorModel != EventType.BASE_SUBSTITUTION) {
                read.setBaseQualities(null, errorModel);
                continue;
            }

            final byte[] quals = read.getBaseQualities(errorModel);
            final int[][] fullReadKeySet = readCovariates.getKeySet(errorModel);                                        // get the keyset for this base using the error model

            final int readLength = read.getReadLength();
            for (int offset = 0; offset < readLength; offset++) {                                                       // recalibrate all bases in the read

                final byte originalQualityScore = quals[offset];

                if (originalQualityScore >= preserveQLessThan) {                                                        // only recalibrate usable qualities (the original quality will come from the instrument -- reported quality)
                    final int[] keySet = fullReadKeySet[offset];                                                        // get the keyset for this base using the error model
                    final byte recalibratedQualityScore = performSequentialQualityCalculation(keySet, errorModel);      // recalibrate the base
                    quals[offset] = recalibratedQualityScore;
                }
            }
            read.setBaseQualities(quals, errorModel);
        }
    }

    /**
     * Implements a serial recalibration of the reads using the combinational table.
     * First, we perform a positional recalibration, and then a subsequent dinuc correction.
     *
     * Given the full recalibration table, we perform the following preprocessing steps:
     *
     * - calculate the global quality score shift across all data [DeltaQ]
     * - calculate for each of cycle and dinuc the shift of the quality scores relative to the global shift
     * -- i.e., DeltaQ(dinuc) = Sum(pos) Sum(Qual) Qempirical(pos, qual, dinuc) - Qreported(pos, qual, dinuc) / Npos * Nqual
     * - The final shift equation is:
     *
     * Qrecal = Qreported + DeltaQ + DeltaQ(pos) + DeltaQ(dinuc) + DeltaQ( ... any other covariate ... )
     * 
     * @param key        The list of Comparables that were calculated from the covariates
     * @param errorModel the event type
     * @return A recalibrated quality score as a byte
     */
    protected byte performSequentialQualityCalculation(final int[] key, final EventType errorModel) {

        final byte qualFromRead = (byte)(long)key[1];
        final double globalDeltaQ = calculateGlobalDeltaQ(recalibrationTables.getTable(RecalibrationTables.TableType.READ_GROUP_TABLE), key, errorModel);
        final double deltaQReported = calculateDeltaQReported(recalibrationTables.getTable(RecalibrationTables.TableType.QUALITY_SCORE_TABLE), key, errorModel, globalDeltaQ, qualFromRead);
        final double deltaQCovariates = calculateDeltaQCovariates(recalibrationTables, key, errorModel, globalDeltaQ, deltaQReported, qualFromRead);

        double recalibratedQual = qualFromRead + globalDeltaQ + deltaQReported + deltaQCovariates;                      // calculate the recalibrated qual using the BQSR formula
        recalibratedQual = QualityUtils.boundQual(MathUtils.fastRound(recalibratedQual), QualityUtils.MAX_RECALIBRATED_Q_SCORE);     // recalibrated quality is bound between 1 and MAX_QUAL

        return quantizationInfo.getQuantizedQuals().get((int) recalibratedQual);                                        // return the quantized version of the recalibrated quality
    }

    private double calculateGlobalDeltaQ(final NestedIntegerArray<RecalibrationDatum> table, final int[] key, final EventType errorModel) {
        double result = 0.0;

        final RecalibrationDatum empiricalQualRG = table.get(key[0], errorModel.index);
        if (empiricalQualRG != null) {
            final double globalDeltaQEmpirical = empiricalQualRG.getEmpiricalQuality();
            final double aggregrateQReported = empiricalQualRG.getEstimatedQReported();
            result = globalDeltaQEmpirical - aggregrateQReported;
        }

        return result;
    }

    private double calculateDeltaQReported(final NestedIntegerArray<RecalibrationDatum> table, final int[] key, final EventType errorModel, final double globalDeltaQ, final byte qualFromRead) {
        double result = 0.0;

        final RecalibrationDatum empiricalQualQS = table.get(key[0], key[1], errorModel.index);
        if (empiricalQualQS != null) {
            final double deltaQReportedEmpirical = empiricalQualQS.getEmpiricalQuality();
            result = deltaQReportedEmpirical - qualFromRead - globalDeltaQ;
        }

        return result;
    }

    private double calculateDeltaQCovariates(final RecalibrationTables recalibrationTables, final int[] key, final EventType errorModel, final double globalDeltaQ, final double deltaQReported, final byte qualFromRead) {
        double result = 0.0;

        // for all optional covariates
        for (int i = 2; i < requestedCovariates.length; i++) {
            if (key[i] < 0)
                continue;

            final RecalibrationDatum empiricalQualCO = recalibrationTables.getTable(i).get(key[0], key[1], key[i], errorModel.index);
            if (empiricalQualCO != null) {
                final double deltaQCovariateEmpirical = empiricalQualCO.getEmpiricalQuality();
                result += (deltaQCovariateEmpirical - qualFromRead - (globalDeltaQ + deltaQReported));
            }
        }
        return result;
    }
}
