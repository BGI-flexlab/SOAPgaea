package org.bgi.flexlab.gaea.tools.baserecalibration.covariates;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMReadGroupRecord;

import java.util.HashMap;
import java.util.List;

import org.bgi.flexlab.gaea.data.structure.bam.GaeaSamRecord;
import org.bgi.flexlab.gaea.tools.baserecalibration.ReadCovariates;
import org.bgi.flexlab.gaea.tools.mapreduce.recalibrator.BaseRecalibrationOptions;

/**
 * The Read Group covariate.
 */

public class ReadGroupCovariate implements RequiredCovariate {

    private final HashMap<String, Integer> readGroupLookupTable = new HashMap<String, Integer>();
    private final HashMap<Integer, String> readGroupReverseLookupTable = new HashMap<Integer, String>();
    private int nextId = 0;

    // Initialize any member variables using the command-line arguments passed to the walkers
    @Override
    public void initialize(final BaseRecalibrationOptions RAC) {}
    //FIXME:add 
    public void initializeReadGroup(SAMFileHeader mFileHeader)
    {
    	List<SAMReadGroupRecord> rg=mFileHeader.getReadGroups();
    	for(SAMReadGroupRecord r:rg){
    		keyForReadGroup(readGroupValueFromRG(r));
    	}
    }
    @Override
    public void recordValues(final GaeaSamRecord read, final ReadCovariates values) {
    	final String readGroupId = readGroupValueFromRG(read.getReadGroup());
        final int key = keyForReadGroup(readGroupId);
        final int l = read.getReadLength();
        for (int i = 0; i < l; i++)
            values.addCovariate(key, key, key, i);
    }

    @Override
    public final Object getValue(final String str) {
        return str;
    }

    @Override
    public String formatKey(final int key) {
        return readGroupReverseLookupTable.get(key);
    }

    @Override
    public int keyFromValue(final Object value) {
        return keyForReadGroup((String) value);
    }

    private int keyForReadGroup(final String readGroupId) {
        if (!readGroupLookupTable.containsKey(readGroupId)) {
            readGroupLookupTable.put(readGroupId, nextId);
            readGroupReverseLookupTable.put(nextId, readGroupId);
            nextId++;
        }
        return readGroupLookupTable.get(readGroupId);
    }

    @Override
    public int maximumKeyValue() {
        return readGroupLookupTable.size() - 1;
    }

    /**
     * If the sample has a PU tag annotation, return that. If not, return the read group id.
     *
     * @param rg the read group record
     * @return platform unit or readgroup id
     */
    private String readGroupValueFromRG(final SAMReadGroupRecord rg) {
        final String platformUnit = rg.getPlatformUnit();
        return platformUnit == null ? rg.getId() : platformUnit;
    }
    
}


