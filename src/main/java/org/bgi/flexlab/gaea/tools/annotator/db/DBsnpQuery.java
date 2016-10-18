package org.bgi.flexlab.gaea.tools.annotator.db;

import java.io.IOException;
import java.util.HashMap;

import org.bgi.flexlab.gaea.tools.annotator.db.Condition.ConditionKey;

public class DBsnpQuery extends AbstractDBQuery {

	private static final long serialVersionUID = -9081062168317341102L;

	@Override
	public boolean check(HashMap<ConditionKey, String> certainValue,
			HashMap<String, String> resultMap) {
		String alt = resultMap.get("ALT");
		if(certainValue.get(ConditionKey.ALT).equalsIgnoreCase(alt)){
			return true;
		}
		return false;
	}
	
	@Override
	public HashMap<String, String> query(Condition condition , String[] fields)throws IOException{
		HashMap<String, String> fieldMap = condition.getFields();
		HashMap<String,String> resultMap = query(condition);
		if (resultMap == null) return null;
		HashMap<String,String> result = new HashMap<String, String>();
		
		for (String field : fields) {
			String dbField = fieldMap.get(field);
			if (dbField.indexOf(':') >= 1) {
				String[] kv= dbField.split(":");
				String subFieldString = resultMap.get(kv[0]);
				HashMap<String,String> subFieldHash = parseSubFieldHash(subFieldString);
				
				result.put(field, subFieldHash.get(kv[1]));
			}else {
				result.put(field, resultMap.get(dbField));
			}
			
		}
		return result; 
	}

	/**
	 * 
	 * @param subFieldString
	 * @return
	 */
	private HashMap<String, String> parseSubFieldHash(String subFieldString){
		HashMap<String,String> subFieldHash = new HashMap<String, String>();
		String[] list = subFieldString.split(";");
		for (String str: list) {
			if(str.indexOf("=")>=0){
				String[] kv = str.split("=");
				subFieldHash.put(kv[0], kv[1]);
			}else {
				subFieldHash.put(str, "TRUE");
			}
		}
		return subFieldHash;
	}
	
}
