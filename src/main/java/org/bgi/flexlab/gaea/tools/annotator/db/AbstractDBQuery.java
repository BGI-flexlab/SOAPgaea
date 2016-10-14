package org.bgi.flexlab.gaea.tools.annotator.db;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.bgi.flexlab.gaea.tools.annotator.config.TableInfo.DbType;
import org.bgi.flexlab.gaea.tools.annotator.db.Condition.ConditionKey;

/**
 * @author huangzhibo
 *
 * 查询数据库... (模版方法模式)
 */
public abstract class AbstractDBQuery implements Serializable {

	private static final long serialVersionUID = -897843908487603204L;
	
	private DBAdapterInterface dbAdapter = null;
	
	public AbstractDBQuery(){
		
	}
	
	public HashMap<String, String> query(Condition condition , String[] fields) throws IOException
	{
		Set<String> dbFields = toDbFields(condition.getFields(), fields); 
		HashMap<String,String> resultMap;
		if (condition.getTableInfo().getIndexTable().isEmpty()) {
			resultMap = dbAdapter.getResult(condition.getTableName(), condition.getConditionString(), dbFields);
			if (check(condition.getConditionMap(),resultMap)) {
				return resultMap;
			}
		}else {
			resultMap = dbAdapter.getResult(condition.getTableInfo().getIndexTable(), condition.getConditionString(), dbFields);
			String keyStr = resultMap.get(condition.getTableInfo().getKey());
			String[] keys = keyStr.split(",");
			for (String key : keys) {
				resultMap = dbAdapter.getResult(condition.getTableInfo().getTable(), key, dbFields);
				if(	check(condition.getConditionMap(), resultMap)){
					return resultMap;
				}
			}
	
		}
		
		return null;
	}

	public abstract boolean check(HashMap<ConditionKey, String> certainValue, HashMap<String, String> resultMap) ;


	private Set<String> toDbFields(HashMap<String, String> fields, String[] fieldList) {
		Set<String> tags = new HashSet<String>();
		for (int i = 0; i < fieldList.length; i++) {
			String fieldValue = fields.get(fieldList[i]);
			if (fieldValue.indexOf(':') >= 1) {
				String[] kv= fieldValue.split(":");
				tags.add(kv[0]);
			}else {
				tags.add(fieldValue);
			}
		}
		return tags;
	}
	
	public void disconnection() throws IOException {
		dbAdapter.disconnection();
	}

	public void connection(String dbName, DbType dbType) throws IOException {
		dbAdapter = DBAdapterFactory.createDbAdapter(dbType);
		dbAdapter.connection(dbName);
	}
	
}
