
package org.bgi.flexlab.gaea.tools.baserecalibration;

import org.bgi.flexlab.gaea.tools.baserecalibration.covariates.Covariate;
import org.bgi.flexlab.gaea.util.EventType;
import org.bgi.flexlab.gaea.util.NestedIntegerArray;

public class RecalibrationTables {

    public enum TableType {
        READ_GROUP_TABLE(0),
        QUALITY_SCORE_TABLE(1),
        OPTIONAL_COVARIATE_TABLES_START(2);

        public final int index;

        private TableType(final int index) {
            this.index = index;
        }
    }

    private final NestedIntegerArray[] tables;

    public RecalibrationTables(final Covariate[] covariates) {
        this(covariates, covariates[TableType.READ_GROUP_TABLE.index].maximumKeyValue() + 1);
    }

    public RecalibrationTables(final Covariate[] covariates, final int numReadGroups) {
        tables = new NestedIntegerArray[covariates.length];

        final int qualDimension = covariates[TableType.QUALITY_SCORE_TABLE.index].maximumKeyValue() + 1;
        final int eventDimension = EventType.values().length;
        tables[TableType.READ_GROUP_TABLE.index] = new NestedIntegerArray<RecalibrationDatum>(numReadGroups, eventDimension);
        tables[TableType.QUALITY_SCORE_TABLE.index] = new NestedIntegerArray<RecalibrationDatum>(numReadGroups, qualDimension, eventDimension);
        for (int i = TableType.OPTIONAL_COVARIATE_TABLES_START.index; i < covariates.length; i++)
            tables[i] = new NestedIntegerArray<RecalibrationDatum>(numReadGroups, qualDimension, covariates[i].maximumKeyValue()+1, eventDimension);
    }

    public NestedIntegerArray<RecalibrationDatum> getTable(final TableType type) {
        return (NestedIntegerArray<RecalibrationDatum>)tables[type.index];
    }

    public NestedIntegerArray<RecalibrationDatum> getTable(final int index) {
        return (NestedIntegerArray<RecalibrationDatum>)tables[index];
    }

    public int numTables() {
        return tables.length;
    }
}
