package org.bgi.flexlab.gaea.tools.recalibrator;

import org.bgi.flexlab.gaea.tools.recalibrator.covariate.Covariate;
import org.bgi.flexlab.gaea.util.NestedObjectArray;

public class RecalibratorTable {
	public enum Type {
		READ_GROUP_TABLE(0), QUALITY_SCORE_TABLE(1), OPTIONAL_COVARIATE_TABLES_START(2);

		public final int index;

		private Type(final int index) {
			this.index = index;
		}
	}
	
	@SuppressWarnings("rawtypes")
	private NestedObjectArray[] tables = null;
	
	public RecalibratorTable(final Covariate[] covariates,int readGroupNumber){
		tables = new NestedObjectArray[covariates.length];
		
		int maxQualityScore = covariates[Type.QUALITY_SCORE_TABLE.index].maximumKeyValue() + 1;
		int eventSize = EventType.values().length;
		
		tables[Type.READ_GROUP_TABLE.index] = new NestedObjectArray<RecalibratorDatum>(readGroupNumber, eventSize);
        tables[Type.QUALITY_SCORE_TABLE.index] = new NestedObjectArray<RecalibratorDatum>(readGroupNumber, maxQualityScore, eventSize);
        for (int i = Type.OPTIONAL_COVARIATE_TABLES_START.index; i < covariates.length; i++)
            tables[i] = new NestedObjectArray<RecalibratorDatum>(readGroupNumber, maxQualityScore, covariates[i].maximumKeyValue()+1, eventSize);
	}
	
	@SuppressWarnings("unchecked")
	public NestedObjectArray<RecalibratorDatum> getTable(int index){
		return (NestedObjectArray<RecalibratorDatum>)tables[index];
	}
	
	public NestedObjectArray<RecalibratorDatum> getTable(Type type){
		return getTable(type.index);
	}
	
	public int length(){
		return tables.length;
	}
}
