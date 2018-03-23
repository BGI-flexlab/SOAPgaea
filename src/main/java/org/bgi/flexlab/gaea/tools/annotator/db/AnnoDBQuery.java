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

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.bgi.flexlab.gaea.tools.annotator.config.DatabaseInfo;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class AnnoDBQuery extends DBQuery {

	public static String INDEX_ALT_COLUMN_NAME = "ALT";

	@Override
	public Results query(Condition condition)throws IOException{
		Results results = new Results();

		HashMap<String,String> result = dbAdapter.getResult(condition.getRefTable().getIndexTable(), condition.getConditionString());
		List<String> alts = condition.getAlts();
		if (result ==null || result.isEmpty()) return null;
		String[] indexAlts = result.get(INDEX_ALT_COLUMN_NAME).split(",");
		for(String alt: alts)
			if(!ArrayUtils.contains(indexAlts, alt))
				return null;

		String mainKeyStr = result.get(condition.getRefTable().getKey());
		String[] mainKeys = mainKeyStr.split(";");

		for(String alt: alts){
			int index = ArrayUtils.indexOf(indexAlts,alt);
			String[] altMainKeys = mainKeys[index].split(",");
			for(String altmk: altMainKeys){
				HashMap<String,String> annoResult = dbAdapter.getResult(condition.getRefTable().getTable(), altmk, condition.getFields());
				if (annoResult ==null || annoResult.isEmpty()){
					System.err.println("Cann't find value from table:"+condition.getRefTable().getTable()+". Key:"+altmk);
					return null;
				}
				results.add(alt, annoResult);
			}
		}
		return results;
	}

	public boolean insert(Condition condition,	Map<String,String>
			fields )throws IOException{
		HashMap<String,String> result = dbAdapter.getResult(condition.getRefTable().getIndexTable(), condition.getConditionString());
		String mainKey = getHashRowKey(condition.getConditionString());
		String alt =  fields.get("ALT");
		Map<String, String> indexKV = new HashMap<>();
		if (result ==null || result.isEmpty()){
			indexKV.put(INDEX_ALT_COLUMN_NAME, alt);
			indexKV.put(condition.getRefTable().getKey(), mainKey);
			dbAdapter.insert(condition.getRefTable().getIndexTable(), condition.getConditionString(), indexKV);
			dbAdapter.insert(condition.getRefTable().getTable(), mainKey, fields);
		}else {
			String altStr = result.get(INDEX_ALT_COLUMN_NAME);
			String mainKeyStr = result.get(condition.getRefTable().getKey());
			String[] alts = altStr.split(",");
			int index = ArrayUtils.indexOf(alts, alt);
			if(index == -1){
				altStr += ","+alt;
				mainKeyStr += ";"+mainKey;
				indexKV.put(INDEX_ALT_COLUMN_NAME, altStr);
				indexKV.put(condition.getRefTable().getKey(), mainKeyStr);
				dbAdapter.insert(condition.getRefTable().getIndexTable(), condition.getConditionString(), indexKV);
				dbAdapter.insert(condition.getRefTable().getTable(), mainKey, fields);
			}
		}
		return true;
	}

	public String getHashRowKey(String key){
		long currentTime = System.currentTimeMillis();
		Random random = new Random();
		currentTime += random.nextInt(1000);
		String mainKey = MD5Hash.getMD5AsHex(Bytes.toBytes(key)).substring(0,8);
		mainKey += MD5Hash.getMD5AsHex(Bytes.toBytes(currentTime)).substring(0,8);
		return mainKey;
	}

	@Override
	public void connection(String dbName, DatabaseInfo.DbType dbType, String connInfo) throws IOException{
		dbAdapter = DBAdapterFactory.createDbAdapter(dbType, connInfo);
		dbAdapter.connection("d");
	}

}
