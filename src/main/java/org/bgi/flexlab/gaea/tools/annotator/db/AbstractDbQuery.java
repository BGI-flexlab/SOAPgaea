package org.bgi.flexlab.gaea.tools.annotator.db;

import java.io.Serializable;
import java.util.HashMap;

import org.bgi.flexlab.gaea.tools.annotator.db.Condition.ConditionKey;

/**
 * @author huangzhibo
 *
 * 查询数据库... (模版方法模式)
 */
public abstract class AbstractDbQuery implements Serializable {

	private static final long serialVersionUID = -897843908487603204L;
	
	private static HashMap<String, DBAdapterInterface> dbAdapterInstance = new HashMap<String, DBAdapterInterface>();
	
	private DBAdapterInterface dbAdapter = null;

	public HashMap<String, String> query(Condition condition , String[] tags)
	{
		if (dbAdapterInstance.containsKey(condition.getDbName())) {
			dbAdapter = dbAdapterInstance.get(condition.getDbName());
		}else {
			dbAdapter = DbAdapterFactory.createDbAdapter(condition.getDbType());
			dbAdapterInstance.put(condition.getDbName(), dbAdapter);
			dbAdapter.connection(condition.getDbName());
		}
		
		HashMap<String,String> resultMap;
		if (condition.getTableInfo().getIndexTable().isEmpty()) {
			resultMap = dbAdapter.getResult(condition.getTableName(), condition.getConditionString(), tags);
			if (check(condition.getConditionMap(),resultMap)) {
				return resultMap;
			}
		}else {
			resultMap = dbAdapter.getResult(condition.getTableInfo().getIndexTable(), condition.getConditionString(), tags);
			String keyStr = resultMap.get(condition.getTableInfo().getKey());
			String[] keys = keyStr.split(",");
			for (String key : keys) {
				resultMap = dbAdapter.getResult(condition.getTableInfo().getIndexTable(), key, tags);
				if(	check(condition.getConditionMap(), resultMap)){
					return resultMap;
				}
			}
	
		}
		
		return null;
	}

	public abstract boolean check(HashMap<ConditionKey, String> certainValue, HashMap<String, String> resultMap) ;
	
}
