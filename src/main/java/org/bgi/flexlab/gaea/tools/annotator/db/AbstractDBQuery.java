package org.bgi.flexlab.gaea.tools.annotator.db;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;

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
	
	public AbstractDBQuery(){}
	
	public HashMap<String, String> query(Condition condition) throws IOException
	{
		HashMap<String,String> resultMap;
		if (condition.getTableInfo().getIndexTable().isEmpty()) {
			resultMap = dbAdapter.getResult(condition.getTableName(), condition.getConditionString());
			if (check(condition.getConditionMap(),resultMap)) {
				return resultMap;
			}
		}else {
			resultMap = dbAdapter.getResult(condition.getTableInfo().getIndexTable(), condition.getConditionString());
			String keyStr = resultMap.get(condition.getTableInfo().getKey());
			String[] keys = keyStr.split(",");
			for (String key : keys) {
				resultMap = dbAdapter.getResult(condition.getTableInfo().getTable(), key);
				if(	check(condition.getConditionMap(), resultMap)){
					return resultMap;
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
		HashMap<String,String> result = new HashMap<String, String>();
		
		for (String field : fields) {
			String dbField = fieldMap.get(field);
			result.put(field, resultMap.getOrDefault(dbField, "."));
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

	public void connection(String dbName, DbType dbType) throws IOException {
		dbAdapter = DBAdapterFactory.createDbAdapter(dbType);
		dbAdapter.connection(dbName);
	}
	
}
