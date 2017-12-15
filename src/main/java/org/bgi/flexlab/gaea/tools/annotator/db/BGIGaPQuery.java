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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.bgi.flexlab.gaea.tools.annotator.config.DatabaseInfo;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

public class BGIGaPQuery extends DBQuery {

	private static final long serialVersionUID = 805441802476032672L;

	@Override
	public Results query(Condition condition)throws IOException{
		HashMap<String, String> fieldMap = condition.getFields();
		Results results = new Results();

		//	keyValue = alt:conditionString
		for (Entry<String, String> keyValue : condition.getConditionHash().entrySet()) {
			HashMap<String,String> result = dbAdapter.getResult(condition.getRefTable().getTable(), keyValue.getValue());
			if (result ==null || result.isEmpty()) return null;
			HashMap<String,String> annoResult = new HashMap<String, String>();
			for (Entry<String, String> entry : fieldMap.entrySet()) {
				annoResult.put(entry.getKey(), result.get(entry.getValue()));
			}

			results.add(keyValue.getKey(), annoResult);
		}

		return results;
	}



	public void connection(String dbName, DatabaseInfo.DbType dbType, String connInfo) throws IOException{
		dbAdapter = DBAdapterFactory.createDbAdapter(dbType, connInfo);
		dbAdapter.connection("base");
	}

}
