package org.bgi.flexlab.gaea.tools.baserecalibration;

import htsjdk.samtools.SAMFileHeader;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.bgi.flexlab.gaea.tools.baserecalibration.covariates.Covariate;
import org.bgi.flexlab.gaea.tools.baserecalibration.covariates.ReadGroupCovariate;
import org.bgi.flexlab.gaea.tools.baserecalibration.report.Report;
import org.bgi.flexlab.gaea.tools.baserecalibration.report.ReportTable;
import org.bgi.flexlab.gaea.tools.mapreduce.recalibrator.BaseRecalibrationOptions;
import org.bgi.flexlab.gaea.util.EventType;
import org.bgi.flexlab.gaea.util.NestedIntegerArray;
import org.bgi.flexlab.gaea.util.Pair;




public class RecalibrationReport {
    private QuantizationInformation quantizationInfo;                                                                          // histogram containing the counts for qual quantization (calculated after recalibration is done)
    private final RecalibrationTables recalibrationTables;                                                              // quick access reference to the tables
    private final Covariate[] requestedCovariates;                                                                      // list of all covariates to be used in this calculation
    private final HashMap<String, Integer> optionalCovariateIndexes;
    private final ReportTable argumentTable;  
    private final BaseRecalibrationOptions RAC;  
    private final int[] tempRGarray = new int[2];
    private final int[] tempQUALarray = new int[3];
    private final int[] tempCOVarray = new int[4];

    public RecalibrationReport(final String RECAL_FILE,final SAMFileHeader mFileHeader) {
    	 final Report report = new Report(RECAL_FILE);
    	argumentTable = report.getTable(RecalibrationUtils.ARGUMENT_REPORT_TABLE_TITLE);
        RAC = initializeArgumentCollectionTable(argumentTable);

        ReportTable quantizedTable = report.getTable(RecalibrationUtils.QUANTIZED_REPORT_TABLE_TITLE);
        quantizationInfo = initializeQuantizationTable(quantizedTable);

        Pair<ArrayList<Covariate>, ArrayList<Covariate>> covariates = RecalibrationUtils.initializeCovariates(RAC);       // initialize the required and optional covariates
        ArrayList<Covariate> requiredCovariates = covariates.getFirst();
        ArrayList<Covariate> optionalCovariates = covariates.getSecond();
        requestedCovariates = new Covariate[requiredCovariates.size() + optionalCovariates.size()];
        optionalCovariateIndexes = new HashMap<String, Integer>(optionalCovariates.size());
        int covariateIndex = 0;
        for (final Covariate covariate : requiredCovariates)
            requestedCovariates[covariateIndex++] = covariate;
        for (final Covariate covariate : optionalCovariates) {
            requestedCovariates[covariateIndex] = covariate;
            final String covariateName = covariate.getClass().getSimpleName().split("Covariate")[0];                    // get the name of the covariate (without the "covariate" part of it) so we can match with the GATKReport
            optionalCovariateIndexes.put(covariateName, covariateIndex-2);
            covariateIndex++;
        }

        for (Covariate cov : requestedCovariates)
        {
        	cov.initialize(RAC);  // initialize any covariate member variables using the shared argument collection
        	if(cov instanceof ReadGroupCovariate)
        		((ReadGroupCovariate) cov).initializeReadGroup(mFileHeader);
        }

        recalibrationTables = new RecalibrationTables(requestedCovariates, countReadGroups(report.getTable(RecalibrationUtils.READGROUP_REPORT_TABLE_TITLE)));

        parseReadGroupTable(report.getTable(RecalibrationUtils.READGROUP_REPORT_TABLE_TITLE), recalibrationTables.getTable(RecalibrationTables.TableType.READ_GROUP_TABLE));

        parseQualityScoreTable(report.getTable(RecalibrationUtils.QUALITY_SCORE_REPORT_TABLE_TITLE), recalibrationTables.getTable(RecalibrationTables.TableType.QUALITY_SCORE_TABLE));

        parseAllCovariatesTable(report.getTable(RecalibrationUtils.ALL_COVARIATES_REPORT_TABLE_TITLE), recalibrationTables);

    }

    protected RecalibrationReport(final QuantizationInformation quantizationInfo, final RecalibrationTables recalibrationTables, final ReportTable argumentTable, final BaseRecalibrationOptions RAC) {
        this.quantizationInfo = quantizationInfo;
        this.recalibrationTables = recalibrationTables;
        this.argumentTable = argumentTable;
        this.RAC = RAC;
        this.requestedCovariates = null;
        this.optionalCovariateIndexes = null;
    }

    /**
     * Counts the number of unique read groups in the table
     *
     * @param reportTable            the GATKReport table containing data for this table
     * @return the number of unique read groups
     */
    private int countReadGroups(final ReportTable reportTable) {
        Set<String> readGroups = new HashSet<String>();
        for ( int i = 0; i < reportTable.getNumRows(); i++ )
            readGroups.add(reportTable.get(i, RecalibrationUtils.READGROUP_COLUMN_NAME).toString());
        return readGroups.size();
    }

    /**
    * Combines two recalibration reports by adding all observations and errors
    *
    * Note: This method DOES NOT recalculate the empirical qualities and quantized qualities. You have to recalculate
    * them after combining. The reason for not calculating it is because this function is inteded for combining a
    * series of recalibration reports, and it only makes sense to calculate the empirical qualities and quantized
    * qualities after all the recalibration reports have been combined. Having the user recalculate when appropriate,
    * makes this method faster
    *
    * Note2: The empirical quality reported, however, is recalculated given its simplicity.
    *
    * @param other the recalibration report to combine with this one
    */
    public void combine(final RecalibrationReport other) {

        for (RecalibrationTables.TableType type : RecalibrationTables.TableType.values()) {
            final NestedIntegerArray<RecalibrationDatum> myTable = recalibrationTables.getTable(type);
            final NestedIntegerArray<RecalibrationDatum> otherTable = other.recalibrationTables.getTable(type);

            for (final NestedIntegerArray.Leaf row : otherTable.getAllLeaves()) {
                final RecalibrationDatum myDatum = myTable.get(row.keys);

                if (myDatum == null)
                    myTable.put((RecalibrationDatum)row.value, row.keys);
                else
                    myDatum.combine((RecalibrationDatum)row.value);
            }
        }
    }

    public QuantizationInformation getQuantizationInfo() {
        return quantizationInfo;
    }

    public RecalibrationTables getRecalibrationTables() {
        return recalibrationTables;
    }

    public Covariate[] getRequestedCovariates() {
        return requestedCovariates;
    }

    /**
     * Compiles the list of keys for the Covariates table and uses the shared parsing utility to produce the actual table
     *
     * @param reportTable            the GATKReport table containing data for this table
     * @param recalibrationTables    the recalibration tables
\     */
    private void parseAllCovariatesTable(final ReportTable reportTable, final RecalibrationTables recalibrationTables) {
        for ( int i = 0; i < reportTable.getNumRows(); i++ ) {
            final Object rg = reportTable.get(i, RecalibrationUtils.READGROUP_COLUMN_NAME);
            tempCOVarray[0] = requestedCovariates[0].keyFromValue(rg);
            final Object qual = reportTable.get(i, RecalibrationUtils.QUALITY_SCORE_COLUMN_NAME);
            tempCOVarray[1] = requestedCovariates[1].keyFromValue(qual);

            final String covName = (String)reportTable.get(i, RecalibrationUtils.COVARIATE_NAME_COLUMN_NAME);
            final int covIndex = optionalCovariateIndexes.get(covName);
            final Object covValue = reportTable.get(i, RecalibrationUtils.COVARIATE_VALUE_COLUMN_NAME);
            tempCOVarray[2] = requestedCovariates[RecalibrationTables.TableType.OPTIONAL_COVARIATE_TABLES_START.index + covIndex].keyFromValue(covValue);

            final EventType event = EventType.eventFrom((String)reportTable.get(i, RecalibrationUtils.EVENT_TYPE_COLUMN_NAME));
            tempCOVarray[3] = event.index;

            recalibrationTables.getTable(RecalibrationTables.TableType.OPTIONAL_COVARIATE_TABLES_START.index + covIndex).put(getRecalibrationDatum(reportTable, i, false), tempCOVarray);
        }
    }

    /**
     *
     * Compiles the list of keys for the QualityScore table and uses the shared parsing utility to produce the actual table
     * @param reportTable            the GATKReport table containing data for this table
     * @param qualTable               the map representing this table
     */
    private void parseQualityScoreTable(final ReportTable reportTable, final NestedIntegerArray<RecalibrationDatum> qualTable) {
        for ( int i = 0; i < reportTable.getNumRows(); i++ ) {
            final Object rg = reportTable.get(i, RecalibrationUtils.READGROUP_COLUMN_NAME);
            tempQUALarray[0] = requestedCovariates[0].keyFromValue(rg);
            final Object qual = reportTable.get(i, RecalibrationUtils.QUALITY_SCORE_COLUMN_NAME);
            tempQUALarray[1] = requestedCovariates[1].keyFromValue(qual);
            final EventType event = EventType.eventFrom((String)reportTable.get(i, RecalibrationUtils.EVENT_TYPE_COLUMN_NAME));
            tempQUALarray[2] = event.index;

            qualTable.put(getRecalibrationDatum(reportTable, i, false), tempQUALarray);
        }
    }

    /**
     * Compiles the list of keys for the ReadGroup table and uses the shared parsing utility to produce the actual table
     *
     * @param reportTable            the GATKReport table containing data for this table
     * @param rgTable                the map representing this table
     */
    private void parseReadGroupTable(final ReportTable reportTable, final NestedIntegerArray<RecalibrationDatum> rgTable) {
    	for ( int i = 0; i < reportTable.getNumRows(); i++ ) {
            final Object rg = reportTable.get(i, RecalibrationUtils.READGROUP_COLUMN_NAME);
            tempRGarray[0] = requestedCovariates[0].keyFromValue(rg);
            final EventType event = EventType.eventFrom((String)reportTable.get(i, RecalibrationUtils.EVENT_TYPE_COLUMN_NAME));
            tempRGarray[1] = event.index;

            rgTable.put(getRecalibrationDatum(reportTable, i, true), tempRGarray);
        }
    }

    private RecalibrationDatum getRecalibrationDatum(final ReportTable reportTable, final int row, final boolean hasEstimatedQReportedColumn) {
        final long nObservations = (Long) reportTable.get(row, RecalibrationUtils.NUMBER_OBSERVATIONS_COLUMN_NAME);
        final long nErrors = (Long) reportTable.get(row, RecalibrationUtils.NUMBER_ERRORS_COLUMN_NAME);
        final double empiricalQuality = (Double) reportTable.get(row, RecalibrationUtils.EMPIRICAL_QUALITY_COLUMN_NAME);

        final double estimatedQReported = hasEstimatedQReportedColumn ?                                                 // the estimatedQreported column only exists in the ReadGroup table
                (Double) reportTable.get(row, RecalibrationUtils.ESTIMATED_Q_REPORTED_COLUMN_NAME) :                      // we get it if we are in the read group table
                Byte.parseByte((String) reportTable.get(row, RecalibrationUtils.QUALITY_SCORE_COLUMN_NAME));              // or we use the reported quality if we are in any other table

        final RecalibrationDatum datum = new RecalibrationDatum(nObservations, nErrors, (byte)1);
        datum.setEstimatedQReported(estimatedQReported);
        datum.setEmpiricalQuality(empiricalQuality);
        return datum;
    }

    /**
     * Parses the quantization table from the GATK Report and turns it into a map of original => quantized quality scores
     *
     * @param table the GATKReportTable containing the quantization mappings
     * @return an ArrayList with the quantization mappings from 0 to MAX_QUAL_SCORE
     */
    private QuantizationInformation initializeQuantizationTable(ReportTable table) {
        final Byte[] quals  = new Byte[QualityUtils.MAX_QUAL_SCORE + 1];
        final Long[] counts = new Long[QualityUtils.MAX_QUAL_SCORE + 1];
        for ( int i = 0; i < table.getNumRows(); i++ ) {
            final byte originalQual = (byte)i;
            final Object quantizedObject = table.get(i, RecalibrationUtils.QUANTIZED_VALUE_COLUMN_NAME);
            final Object countObject = table.get(i, RecalibrationUtils.QUANTIZED_COUNT_COLUMN_NAME);
            final byte quantizedQual = Byte.parseByte(quantizedObject.toString());
            final long quantizedCount = Long.parseLong(countObject.toString());
            quals[originalQual] = quantizedQual;
            counts[originalQual] = quantizedCount;
        }
        return new QuantizationInformation(Arrays.asList(quals), Arrays.asList(counts));
    }

    /**
     * Parses the arguments table from the GATK Report and creates a RAC object with the proper initialization values
     *
     * @param table the GATKReportTable containing the arguments and its corresponding values
     * @return a RAC object properly initialized with all the objects in the table
     */
    private BaseRecalibrationOptions initializeArgumentCollectionTable(ReportTable table) {
        final BaseRecalibrationOptions RAC = new BaseRecalibrationOptions();

        for ( int i = 0; i < table.getNumRows(); i++ ) {
            final String argument = table.get(i, "Argument").toString();
            Object value = table.get(i, RecalibrationUtils.ARGUMENT_VALUE_COLUMN_NAME);
            if (value.equals("null"))
                value = null;                                                                                           // generic translation of null values that were printed out as strings | todo -- add this capability to the GATKReport

            if (argument.equals("covariate") && value != null)
                RAC.COVARIATES = value.toString().split(",");

            else if (argument.equals("standard_covs"))
                RAC.DO_NOT_USE_STANDARD_COVARIATES = Boolean.parseBoolean((String) value);

            else if (argument.equals("solid_recal_mode"))
                RAC.SOLID_RECAL_MODE = RecalibrationUtils.SOLID_RECAL_MODE.recalModeFromString((String) value);

            else if (argument.equals("solid_nocall_strategy"))
                RAC.SOLID_NOCALL_STRATEGY = RecalibrationUtils.SOLID_NOCALL_STRATEGY.nocallStrategyFromString((String) value);

            else if (argument.equals("mismatches_context_size"))
                RAC.MISMATCHES_CONTEXT_SIZE = Integer.parseInt((String) value);

            else if (argument.equals("indels_context_size"))
                RAC.INDELS_CONTEXT_SIZE = Integer.parseInt((String) value);

            else if (argument.equals("mismatches_default_quality"))
                RAC.MISMATCHES_DEFAULT_QUALITY = Byte.parseByte((String) value);

            else if (argument.equals("insertions_default_quality"))
                RAC.INSERTIONS_DEFAULT_QUALITY = Byte.parseByte((String) value);

            else if (argument.equals("deletions_default_quality"))
                RAC.DELETIONS_DEFAULT_QUALITY = Byte.parseByte((String) value);

            else if (argument.equals("low_quality_tail"))
                RAC.LOW_QUAL_TAIL = Byte.parseByte((String) value);

            else if (argument.equals("default_platform"))
                RAC.DEFAULT_PLATFORM = (String) value;

            else if (argument.equals("force_platform"))
                RAC.FORCE_PLATFORM = (String) value;

            else if (argument.equals("quantizing_levels"))
                RAC.QUANTIZING_LEVELS = Integer.parseInt((String) value);

            else if (argument.equals("keep_intermediate_files"))
                RAC.KEEP_INTERMEDIATE_FILES = Boolean.parseBoolean((String) value);

            else if (argument.equals("no_plots"))
                RAC.NO_PLOTS = Boolean.parseBoolean((String) value);

          //  else if (argument.equals("recalibration_report"))
           //     RAC.recalibrationReport = (value == null) ? null : new File((String) value);

            else if (argument.equals("binary_tag_name"))
                RAC.BINARY_TAG_NAME = (value == null) ? null : (String) value;
        }

        return RAC;
    }

    /**
     * this functionality avoids recalculating the empirical qualities, estimated reported quality
     * and quantization of the quality scores during every call of combine(). Very useful for the BQSRGatherer.
     */
    public void calculateQuantizedQualities() {
        quantizationInfo = new QuantizationInformation(recalibrationTables, RAC.QUANTIZING_LEVELS);
    }

    public void output(PrintStream output) {
        RecalibrationUtils.outputRecalibrationReport(argumentTable, quantizationInfo, recalibrationTables, requestedCovariates, output);
    }

    public BaseRecalibrationOptions getRAC() {
        return RAC;
    }

    public Covariate[] getCovariates() {
        return requestedCovariates;
    }
}
