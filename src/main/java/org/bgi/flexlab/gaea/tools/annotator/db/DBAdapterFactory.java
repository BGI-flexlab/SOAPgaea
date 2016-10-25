package org.bgi.flexlab.gaea.tools.annotator.db;

import org.bgi.flexlab.gaea.tools.annotator.config.DatabaseInfo.DbType;

public class DBAdapterFactory {
	
	/**
	 * 根据 dbType 创建 AbstractDBAdapter 实例
	 * @param connInfo 
	 * @param dbType 
	 * @param databaseInfo
	 * @return
	 */
	public static DBAdapterInterface createDbAdapter(DbType dbType, String connInfo ) {
		if(dbType == DbType.HBASE){
			HbaseAdapter hbase = new HbaseAdapter(connInfo);
			return hbase;
		}else if (dbType == DbType.MYSQL) {
			return new MysqlAdapter(connInfo);
		}
		return new HbaseAdapter(connInfo);
	}

}
