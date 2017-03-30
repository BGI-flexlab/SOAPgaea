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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

public class DBNSFPQuery extends DBQuery {

	private static final long serialVersionUID = 7515609026719464241L;
	
	@Override
	public Results query(Condition condition)throws IOException{
		HashMap<String, String> fieldMap = condition.getFields();
		Results results = new Results();
		
		//	keyValue = alt:conditionString
		for (Entry<String, String> keyValue : condition.getConditionHash().entrySet()) {
			HashMap<String,String> result = dbAdapter.getResult(condition.getRefTable().getIndexTable(), keyValue.getValue());
			if (result ==null || result.isEmpty()) return null;
			String keyStr = result.get(condition.getRefTable().getKey());
			result = dbAdapter.getResult(condition.getRefTable().getTable(), keyStr);
			HashMap<String,String> annoResult = new HashMap<String, String>();
			for (Entry<String, String> entry : fieldMap.entrySet()) {
				annoResult.put(entry.getKey(), result.get(entry.getValue()));
			}
			
			results.add(keyValue.getKey(), annoResult);
		}
			
		return results;
	}

}
