package org.bgi.flexlab.gaea.tools.annotator.db;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

public class DBNSFPQuery extends DBQuery {

	private static final long serialVersionUID = 7515609026719464241L;
	
	@Override
	public Results query(Condition condition)throws IOException{
		HashMap<String, String> fieldMap = condition.getFields();
		Results results = new Results();
		
		//	keyValue = alt:conditionString
		for (Entry<String, String> keyValue : condition.getConditionHash().entrySet()) {
			HashMap<String,String> result = dbAdapter.getResult(condition.getRefTable().getIndexTable(), keyValue.getValue());
			if (result ==null || result.isEmpty()) return null;
			String keyStr = result.get(condition.getRefTable().getKey());
			result = dbAdapter.getResult(condition.getRefTable().getTable(), keyStr);
			HashMap<String,String> annoResult = new HashMap<String, String>();
			for (Entry<String, String> entry : fieldMap.entrySet()) {
				annoResult.put(entry.getKey(), result.get(entry.getValue()));
			}
			
			results.add(keyValue.getKey(), annoResult);
		}
			
		return results;
	}

}
