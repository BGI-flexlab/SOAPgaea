package org.bgi.flexlab.gaea.tools.annotator.db;


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
//			result.put(condition.getRefTable().getKey(), key);
		String altStr = result.get("ALT");
		if (altStr == null) {
			System.err.println("Alt is null:"+condition.getRefTable().getTable()+". Key:"+key);
			return null;
		}

		if (!altStr.contains(",")) {
			altStr = altStr.toUpperCase();
			if(alts.contains(altStr)){
				results.add(altStr, annoResult);
			}
		}else {
			String[] alt_list = altStr.split(",");
//				splitResult();
			for (String alt : alt_list) {
				alt = alt.toUpperCase();
				if(alts.contains(alt)){
					results.add(alt, annoResult);
				}
			}
		}


		return results;
	}
	
	

	

}
