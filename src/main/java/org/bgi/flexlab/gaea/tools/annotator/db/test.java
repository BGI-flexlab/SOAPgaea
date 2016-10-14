package org.bgi.flexlab.gaea.tools.annotator.db;

import org.bgi.flexlab.gaea.tools.annotator.config.TableInfo.DbType;

public class DBAdapterFactory {
	
	/**
	 * 根据 dbType 创建 AbstractDBAdapter 实例
	 * @param dbType
	 * @return
	 */
	public static DBAdapterInterface createDbAdapter(DbType dbType) {
		if(dbType == DbType.HBASE)
		{
			return new HbaseAdapter();
		}else if (dbType == DbType.MYSQL) {
			return new MysqlAdapter();
		}
		return new HbaseAdapter();
	}

}
