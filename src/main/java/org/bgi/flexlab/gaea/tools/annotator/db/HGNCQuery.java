package org.bgi.flexlab.gaea.tools.annotator.db;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;

import org.bgi.flexlab.gaea.tools.annotator.effect.AnnotationContext;

public class HGNCQuery extends DBQuery {

	private static final long serialVersionUID = 805441802476032672L;

	@Override
	public Results query(Condition condition)throws IOException{
		HashMap<String, String> fieldMap = condition.getFields();
		Results results = new Results();

		for (String gene : condition.getGenes()) {
			HashMap<String,String> result = dbAdapter.getResult(condition.getRefTable().getTable(), gene);
			if (result ==null || result.isEmpty()) return null;
				
			HashMap<String,String> annoResult = new HashMap<String, String>();
			for (Entry<String, String> entry : fieldMap.entrySet()) {
				annoResult.put(entry.getKey(), result.get(entry.getValue()));
			}
			results.add(gene, annoResult);
		}
		return results;
	}
	
	
	@Override
	LinkedList<HashMap<String, String>> getAcResultList(AnnotationContext ac) {
		LinkedList<HashMap<String, String>> resultList = results.get(ac.getGeneName());
		return resultList;
	}
	

}
