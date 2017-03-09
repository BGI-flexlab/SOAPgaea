package org.bgi.flexlab.gaea.tools.annotator.db;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

public class GwasQuery extends DBQuery {

	private static final long serialVersionUID = 805441802476032672L;

	@Override
	public Results query(Condition condition)throws IOException{
		HashMap<String, String> fieldMap = condition.getFields();
		Results results = new Results();

		HashMap<String,String> result = dbAdapter.getResult(condition.getRefTable().getIndexTable(), condition.getConditionString());
		if (result ==null || result.isEmpty()) return null;
		String keyStr = result.get(condition.getRefTable().getKey());
		String[] keys = keyStr.split(",");
		for (String key : keys) {
			result = dbAdapter.getResult(condition.getRefTable().getTable(), key);
			
			HashMap<String,String> annoResult = new HashMap<String, String>();
			for (Entry<String, String> entry : fieldMap.entrySet()) {
				annoResult.put(entry.getKey(), result.get(entry.getValue()));
			}
			
			if (result ==null || result.isEmpty()){
				System.err.println("Cann't find value from table:"+condition.getRefTable().getTable()+". Key:"+key);
				return null;
			}
			
			for (String alt : condition.getAlts()) {
				results.add(alt, annoResult);
			}
		}
		
		return results;
	}

}
