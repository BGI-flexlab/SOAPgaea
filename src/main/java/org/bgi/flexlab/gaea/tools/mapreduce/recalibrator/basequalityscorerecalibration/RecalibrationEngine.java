package org.bgi.flexlab.gaea.tools.mapreduce.recalibrator.basequalityscorerecalibration;

import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.data.structure.pileup.PileupElement;
import org.bgi.flexlab.gaea.tools.baserecalibration.ReadCovariates;
import org.bgi.flexlab.gaea.tools.baserecalibration.RecalibrationDatum;
import org.bgi.flexlab.gaea.tools.baserecalibration.RecalibrationTables;
import org.bgi.flexlab.gaea.tools.baserecalibration.covariates.Covariate;
import org.bgi.flexlab.gaea.util.BaseUtils;
import org.bgi.flexlab.gaea.util.EventType;
import org.bgi.flexlab.gaea.util.NestedIntegerArray;

public class RecalibrationEngine {
	protected Covariate[] covariates;
    protected RecalibrationTables recalibrationTables;

    public void initialize(final Covariate[] covariates, final RecalibrationTables recalibrationTables) {
        this.covariates = covariates;
        this.recalibrationTables = recalibrationTables;
    }

    
    public  void updateDataForPileupElement(final PileupElement pileupElement, final byte refBase) {
        final int offset = pileupElement.getOffset();
        final ReadCovariates readCovariates = covariateKeySetFrom(pileupElement.getRead());
        
        final byte qual = pileupElement.getQuality();
        final boolean isError = !BaseUtils.basesAreEqual(pileupElement.getBase(), refBase);
        final int[] keys = readCovariates.getKeySet(offset, EventType.BASE_SUBSTITUTION);
        final int eventIndex = EventType.BASE_SUBSTITUTION.index;

        updateRgDatum(keys[0], eventIndex, qual, isError);
        updateQualDatum(keys, eventIndex, qual, isError);
        updateOtherDatum(keys, eventIndex, qual, isError);
        //readCovariates.clear();
    }
    
    protected RecalibrationDatum createDatumObject(final byte reportedQual, final boolean isError) {
        return new RecalibrationDatum(1, isError ? 1:0, reportedQual);
    }
    
    protected ReadCovariates covariateKeySetFrom(GaeaSamRecord read) {
        return (ReadCovariates) read.getTemporaryAttribute(BaseRecalibrator.COVARS_ATTRIBUTE);
    }
    
    private void updateRgDatum(int key, int eventIndex, byte qual, boolean isError) {
    	 final NestedIntegerArray<RecalibrationDatum> rgRecalTable = recalibrationTables.getTable(RecalibrationTables.TableType.READ_GROUP_TABLE);
         
         final RecalibrationDatum rgPreviousDatum = rgRecalTable.get(key, eventIndex);
         final RecalibrationDatum rgThisDatum = createDatumObject(qual, isError);
         if (rgPreviousDatum == null)                                                                                // key doesn't exist yet in the map so make a new bucket and add it
             rgRecalTable.put(rgThisDatum, key, eventIndex);
         else
             rgPreviousDatum.combine(rgThisDatum);
    }
    
    private void updateQualDatum(int[] keys, int eventIndex, byte qual, boolean isError) {
    	final NestedIntegerArray<RecalibrationDatum> qualRecalTable = recalibrationTables.getTable(RecalibrationTables.TableType.QUALITY_SCORE_TABLE);
        final RecalibrationDatum qualPreviousDatum = qualRecalTable.get(keys[0], keys[1], eventIndex);
        if (qualPreviousDatum == null)
            qualRecalTable.put(createDatumObject(qual, isError), keys[0], keys[1], eventIndex);
        else
            qualPreviousDatum.increment(isError);
    }
    
    private void updateOtherDatum(int[] keys, int eventIndex, byte qual, boolean isError) {
    	 for (int i = 2; i < covariates.length; i++) {
             if (keys[i] < 0)
                 continue;
             final NestedIntegerArray<RecalibrationDatum> covRecalTable = recalibrationTables.getTable(i);
             final RecalibrationDatum covPreviousDatum = covRecalTable.get(keys[0], keys[1], keys[i], eventIndex);
             if (covPreviousDatum == null)
                 covRecalTable.put(createDatumObject(qual, isError), keys[0], keys[1], keys[i], eventIndex);
             else
                 covPreviousDatum.increment(isError);
         }
    }
}
