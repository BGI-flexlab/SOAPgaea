package org.bgi.flexlab.gaea.tools.annotator.db;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;

import org.bgi.flexlab.gaea.tools.annotator.effect.AnnotationContext;

public class IndexGeneDBQuery extends DBQuery {

	private static final long serialVersionUID = 7515609026719464241L;
	
	@Override
	public Results query(Condition condition)throws IOException{
		HashMap<String, String> fieldMap = condition.getFields();
		Results results = new Results();
		
		// entry = alt:conditionString, gene:conditionString		
		for(Entry<String, String> entry : condition.getConditionHash().entrySet()){
			HashMap<String,String> result = dbAdapter.getResult(condition.getRefTable().getIndexTable(), entry.getValue());
			if (result ==null || result.isEmpty()) continue;
			String key = result.get(condition.getRefTable().getKey());
			result = dbAdapter.getResult(condition.getRefTable().getTable(), key, fieldMap);
			if (result ==null || result.isEmpty()) continue;
			results.add(entry.getKey(), result);
		}
		
		return results;
	}

	@Override
	LinkedList<HashMap<String, String>> getAcResultList(
			AnnotationContext annotationContext) {
		LinkedList<HashMap<String, String>> resultList = results.get(annotationContext.getGeneName());
		return resultList;
	}

}
