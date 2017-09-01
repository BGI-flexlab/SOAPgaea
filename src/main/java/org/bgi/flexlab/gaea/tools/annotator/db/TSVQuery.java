package org.bgi.flexlab.gaea.tools.annotator.db;


import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

/**
 * @author huangzhibo
 *
 */
public class TSVQuery extends DBQuery {

	private static final long serialVersionUID = 805441802476012341L;

	/**
	 * 根据condition查询数据库
	 * @param condition
	 * @return
	 * @throws IOException
	 */
	public Results query(Condition condition)throws IOException{
		HashMap<String, String> fieldMap = condition.getFields();
		Results results = new Results();

		HashMap<String,String> result = dbAdapter.getResult(condition.getRefTable().getIndexTable(), condition.getConditionString());
		if (result ==null || result.isEmpty()) return null;
		List<String> alts = condition.getAlts();

		String keyStr = result.get(condition.getConditionString());

		String[] keys = keyStr.split(",");
		for (String key : keys) {
			result = dbAdapter.getResult("data", key);

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
		}

		return results;
	}
	
}
