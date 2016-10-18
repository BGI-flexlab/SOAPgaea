package org.bgi.flexlab.gaea.tools.baserecalibration;


import java.util.Arrays;
import java.util.List;

import org.bgi.flexlab.gaea.tools.baserecalibration.report.ReportTable;
import org.bgi.flexlab.gaea.util.MathUtils;
import org.bgi.flexlab.gaea.util.NestedIntegerArray;




public class QuantizationInformation {
    private List<Byte> quantizedQuals;
    private List<Long> empiricalQualCounts;
    private int quantizationLevels;

    private QuantizationInformation(List<Byte> quantizedQuals, List<Long> empiricalQualCounts, int quantizationLevels) {
        this.quantizedQuals = quantizedQuals;
        this.empiricalQualCounts = empiricalQualCounts;
        this.quantizationLevels = quantizationLevels;
    }

    public QuantizationInformation(List<Byte> quantizedQuals, List<Long> empiricalQualCounts) {
        this(quantizedQuals, empiricalQualCounts, calculateQuantizationLevels(quantizedQuals));
    }
    
    public QuantizationInformation(final RecalibrationTables recalibrationTables, final int quantizationLevels) {
        final Long [] qualHistogram = new Long[QualityUtils.MAX_QUAL_SCORE+1];                                          // create a histogram with the empirical quality distribution
        for (int i = 0; i < qualHistogram.length; i++)
            qualHistogram[i] = 0L;

        final NestedIntegerArray<RecalibrationDatum> qualTable = recalibrationTables.getTable(RecalibrationTables.TableType.QUALITY_SCORE_TABLE); // get the quality score table

        for (final RecalibrationDatum value : qualTable.getAllValues()) {
            final RecalibrationDatum datum = value;
            final int empiricalQual = MathUtils.fastRound(datum.getEmpiricalQuality());                                 // convert the empirical quality to an integer ( it is already capped by MAX_QUAL )
            qualHistogram[empiricalQual] += datum.getNumObservations();                                                      // add the number of observations for every key
        }
        empiricalQualCounts = Arrays.asList(qualHistogram);                                                             // histogram with the number of observations of the empirical qualities
        quantizeQualityScores(quantizationLevels);

        this.quantizationLevels = quantizationLevels;
    }


    public void quantizeQualityScores(int nLevels) {
        QualityQuantizer quantizer = new QualityQuantizer(empiricalQualCounts, nLevels, QualityUtils.MIN_USABLE_Q_SCORE);     // quantize the qualities to the desired number of levels
        quantizedQuals = quantizer.getOriginalToQuantizedMap();                                                         // map with the original to quantized qual map (using the standard number of levels in the RAC)
    }

    public void noQuantization() {
        this.quantizationLevels = QualityUtils.MAX_QUAL_SCORE;
        for (int i = 0; i < this.quantizationLevels; i++)
            quantizedQuals.set(i, (byte) i);
    }

    public List<Byte> getQuantizedQuals() {
        return quantizedQuals;
    }

    public int getQuantizationLevels() {
        return quantizationLevels;
    }

    public ReportTable generateReportTable() {
    	ReportTable quantizedTable = new ReportTable(RecalibrationUtils.QUANTIZED_REPORT_TABLE_TITLE, "Quality quantization map", 3);
        quantizedTable.addColumn(RecalibrationUtils.QUALITY_SCORE_COLUMN_NAME);
        quantizedTable.addColumn(RecalibrationUtils.QUANTIZED_COUNT_COLUMN_NAME);
        quantizedTable.addColumn(RecalibrationUtils.QUANTIZED_VALUE_COLUMN_NAME);
        for (int qual = 0; qual <= QualityUtils.MAX_QUAL_SCORE; qual++) {
            quantizedTable.set(qual, RecalibrationUtils.QUALITY_SCORE_COLUMN_NAME, qual);
            quantizedTable.set(qual, RecalibrationUtils.QUANTIZED_COUNT_COLUMN_NAME, empiricalQualCounts.get(qual));
        	quantizedTable.set(qual, RecalibrationUtils.QUANTIZED_VALUE_COLUMN_NAME, quantizedQuals.get(qual));
        }
      return quantizedTable;
    }

    private static int calculateQuantizationLevels(List<Byte> quantizedQuals) {
        byte lastByte = -1;
        int quantizationLevels = 0;
        for (byte q : quantizedQuals) {
            if (q != lastByte) {
                quantizationLevels++;
                lastByte = q;
            }
        }
        return quantizationLevels;
    }
}
