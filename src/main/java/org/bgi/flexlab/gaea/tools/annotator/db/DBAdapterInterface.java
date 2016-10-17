package org.bgi.flexlab.gaea.tools.annotator.db;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

public interface DBAdapterInterface {
	
	public void connection(String dbName) throws IOException;
	
	public void disconnection() throws IOException;
	
	public HashMap<String, String> getResult(String tableName, String condition, Set<String> tags) throws IOException;

	 /**
     * 根据查询条件查询所有相关信息，返回HashMap
     * 
     * @tableName 表名
     * @conditionString conditionString (对于Hbase则为rowKey)
     * @return HashMap<String, String>
     */
	public HashMap<String, String> getResult(String tableName,
			String conditionString) throws IOException;

}
