/*******************************************************************************
 * Copyright (c) 2017, BGI-Shenzhen
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 *******************************************************************************/
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
		}else if (dbType == DbType.TSV) {
			return new TSVAdapter(connInfo);
		}else if (dbType == DbType.VCF) {
			return new VCFAdapter(connInfo);
		}
		return new MysqlAdapter(connInfo);
	}

}
