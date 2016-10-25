package org.bgi.flexlab.gaea.tools.annotator.db;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.bgi.flexlab.gaea.tools.annotator.config.DatabaseInfo.DbType;
import org.bgi.flexlab.gaea.tools.annotator.db.Condition.ConditionKey;

/**
 * @author huangzhibo
 *
 * 查询数据库... (模版方法模式)
 */
public abstract class AbstractDBQuery implements Serializable {

	private static final long serialVersionUID = -897843908487603204L;
	private static HashMap<String, List<HashMap<String,String>>> results = new HashMap<String, List<HashMap<String,String>>>();
	
	private DBAdapterInterface dbAdapter = null;
	
	public AbstractDBQuery(){}
	
	public static void cleanResults() {
		if (results.size()>500) {
			results.clear();
		}
	}
	
	public HashMap<String, String> query(Condition condition) throws IOException {
		if(dbAdapter == null) return null;
		List<HashMap<String,String>> resultMapList = new ArrayList<HashMap<String,String>>();
		HashMap<String,String> resultMap;
		
		if (condition.getRefTable().getIndexTable().isEmpty()) {
			resultMap = dbAdapter.getResult(condition.getRefTable().getTable(), condition.getConditionString());
			if (check(condition.getConditionMap(),resultMap)) {
				return resultMap;
			}
		}else {
			String conditionHashKey = condition.getRefTable().getIndexTable() + condition.getConditionString();
			if (results.containsKey(conditionHashKey)) {
				resultMapList = results.get(conditionHashKey);
			}else if(condition.getConditionString() != null && !condition.getConditionString().isEmpty()) {
					
				resultMap = dbAdapter.getResult(condition.getRefTable().getIndexTable(), condition.getConditionString());
				if (resultMap ==null || resultMap.isEmpty()) return null;
				String keyStr = resultMap.get(condition.getRefTable().getKey());
				String[] keys = keyStr.split(",");
				for (String key : keys) {
					resultMap = dbAdapter.getResult(condition.getRefTable().getTable(), key);
					resultMapList.add(resultMap);
				}
			}
			for (HashMap<String,String> resultHash : resultMapList) {
				if(	check(condition.getConditionMap(), resultHash)){
					return resultHash;
				}
			}
		}
		return null;
	}
	
	/**
	 * 根据user.config中的fields列表返回对应的哈希表（field为key）
	 * 
	 * @param condition
	 * @param fields
	 * @return
	 * @throws IOException
	 */
	public HashMap<String, String> query(Condition condition , String[] fields)throws IOException{
		HashMap<String, String> fieldMap = condition.getFields();
		HashMap<String,String> resultMap = query(condition);
		if (resultMap == null) return null;
		HashMap<String,String> result = new HashMap<String, String>();
		
		for (String field : fields) {
			String dbField = fieldMap.get(field);
			result.put(field, resultMap.get(dbField));
		}
		return result; 
	}

	/**
	 * 判断所查询出的行信息是否符合要求
	 * @param certainValue
	 * @param resultMap
	 * @return
	 */
	public abstract boolean check(HashMap<ConditionKey, String> certainValue, HashMap<String, String> resultMap) ;
	
	public void disconnection() throws IOException {
		dbAdapter.disconnection();
	}

	public void connection(String dbName, DbType dbType, String connInfo) throws IOException{
		dbAdapter = DBAdapterFactory.createDbAdapter(dbType, connInfo);
		dbAdapter.connection(dbName);
	}
	
}
