package org.bgi.flexlab.gaea.tools.annotator.db;

import java.io.IOException;
import java.util.HashMap;

public interface DBAdapterInterface {
	
	public void connection(String dbName) throws IOException;
	
	public void disconnection() throws IOException;
	
	/**
	 * 根据查询条件和fieldMap查询所有相关字段，fieldMap的value为数据库中字段String，其key值作为返回的HashMap的key。
	 * 
	 * @param tableName
	 * @param condition
	 * @param fieldMap  
	 * @return
	 * @throws IOException
	 */
	public HashMap<String, String> getResult(String tableName, String condition, HashMap<String, String> fieldMap) throws IOException;

	 /**
     * 根据查询条件查询所有相关信息，返回的HashMap包含该行所有字段
     * 
     * @tableName 表名
     * @conditionString conditionString (对于Hbase则为rowKey)
     * @return HashMap<String, String>
     */
	public HashMap<String, String> getResult(String tableName,
			String conditionString) throws IOException;

}
