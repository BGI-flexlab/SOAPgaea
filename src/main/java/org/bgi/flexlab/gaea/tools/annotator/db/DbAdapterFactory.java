package org.bgi.flexlab.gaea.tools.annotator.db;

import org.bgi.flexlab.gaea.tools.annotator.config.TableInfo.DbType;

public class DbAdapterFactory {
	
	
	public static DBAdapterInterface createDbAdapter(DbType dbType) {
		if(dbType == DbType.HBASE)
		{
			return new HbaseAdapter();
		}else if (dbType == DbType.MYSQL) {
			return new MysqlAdapter();
		}else {
//			System.out.println("dbType is wrong!");
		}
		return new HbaseAdapter();
	}
	
	

}
