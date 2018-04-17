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


import org.bgi.flexlab.gaea.tools.annotator.config.DatabaseInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VCFQuery extends DBQuery {

	private static final long serialVersionUID = 805441802476032672L;

	@Override
	public Results query(Condition condition)throws IOException{
		List<String> fields = condition.getFields();
		Results results = new Results();
		List<String> alts = condition.getAlts();
		String key = condition.getConditionString();
		HashMap<String,String> result = dbAdapter.getResult(condition.getRefTable().getTable(), key);

		if (result ==null || result.isEmpty())
			return null;

		HashMap<String,String> annoResult = new HashMap<>();
		for (String field : fields) {
			annoResult.put(field, result.get(field));
		}

		String resultAltStr = result.get("ALT");
		if (resultAltStr == null) {
			System.err.println("Alt is null:"+condition.getRefTable().getTable()+". Key:"+key);
			return null;
		}

		if (!resultAltStr.contains(",")) {
			resultAltStr = resultAltStr.toUpperCase();
			if(alts.contains(resultAltStr)){
				results.add(resultAltStr, annoResult);
			}
		}else {
			String[] resultAlts = resultAltStr.split(",");
			List<HashMap<String, String>> annoResults = splitResult(annoResult, resultAlts.length);
			for (int i = 0; i < resultAlts.length; i++) {
				String alt = resultAlts[i].toUpperCase();
				if(alts.contains(alt)){
					results.add(alt, annoResults.get(i));
				}
			}
		}

		return results;
	}

	@Override
	protected List<HashMap<String, String>> splitResult(HashMap<String, String> result, int altNum) {
		List<HashMap<String, String>> resultList = new ArrayList<>();
		for (int i = 0; i < altNum; i++) {
			resultList.add(new HashMap<>());
		}

		for (Map.Entry<String, String> entry : result.entrySet()) {
			String v = entry.getValue();
			if(v == null) continue;
			if(v.startsWith("[") && v.endsWith("]")){
				v = v.substring(1,v.length()-1);

				String[] values = v.split(",");
				if(altNum == values.length){
					for (int i = 0; i < altNum; i++) {
						resultList.get(i).put(entry.getKey(), values[i]);
					}
				}else {
					for (int i = 0; i < altNum; i++) {
						resultList.get(i).put(entry.getKey(), v);
					}
				}
			}else {
				for (int i = 0; i < altNum; i++) {
					resultList.get(i).put(entry.getKey(), entry.getValue());
				}
			}
		}
		return resultList;
	}

	public void connection(String dbName, DatabaseInfo.DbType dbType, String connInfo) throws IOException{
		dbAdapter = DBAdapterFactory.createDbAdapter(dbType, connInfo);
		dbAdapter.connection(dbName);
	}
}
