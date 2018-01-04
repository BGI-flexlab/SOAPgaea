package org.bgi.flexlab.gaea.tools.annotator.db;


import org.bgi.flexlab.gaea.tools.annotator.config.DatabaseInfo;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

public class VCFQuery extends DBQuery {

	private static final long serialVersionUID = 805441802476032672L;

	@Override
	public Results query(Condition condition)throws IOException{
		HashMap<String, String> fieldMap = condition.getFields();
		Results results = new Results();
		List<String> alts = condition.getAlts();
		String key = condition.getConditionString();
		HashMap<String,String> result = dbAdapter.getResult(condition.getRefTable().getTable(), key);

		HashMap<String,String> annoResult = new HashMap<String, String>();
		for (Entry<String, String> entry : fieldMap.entrySet()) {
			annoResult.put(entry.getKey(), result.get(entry.getValue()));
		}

		if (result ==null || result.isEmpty()){
			System.err.println("Cann't find value from table:"+condition.getRefTable().getTable()+". Key:"+key);
			return null;
		}

		String resultAltStr = result.get("ALT");
		if (resultAltStr == null) {
			System.err.println("Alt is null:"+condition.getRefTable().getTable()+". Key:"+key);
			return null;
		}

		if (!resultAltStr.contains(",")) {
			resultAltStr = resultAltStr.toUpperCase();
			if(alts.contains(resultAltStr)){
				results.add(resultAltStr, annoResult);
			}
		}else {
			String[] resultAlts = resultAltStr.split(",");
			List<HashMap<String, String>> annoResults = splitResult(annoResult, resultAlts.length);
			for (int i = 0; i < resultAlts.length; i++) {
				String alt = resultAlts[i].toUpperCase();
				if(alts.contains(alt)){
					results.add(alt, annoResults.get(i));
				}
			}
		}

		return results;
	}

	public void connection(String dbName, DatabaseInfo.DbType dbType, String connInfo) throws IOException{
		dbAdapter = DBAdapterFactory.createDbAdapter(dbType, connInfo);
		dbAdapter.connection(dbName);
	}
}
