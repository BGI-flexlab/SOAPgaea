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
import java.util.LinkedList;
import java.util.Map.Entry;

import org.bgi.flexlab.gaea.tools.annotator.effect.AnnotationContext;

public class IndexGeneDBQuery extends DBQuery {

	private static final long serialVersionUID = 7515609026719464241L;
	
	@Override
	public Results query(Condition condition)throws IOException{
		HashMap<String, String> fieldMap = condition.getFields();
		Results results = new Results();
		
		// entry = alt:conditionString, gene:conditionString		
		for(Entry<String, String> entry : condition.getConditionHash().entrySet()){
			HashMap<String,String> result = dbAdapter.getResult(condition.getRefTable().getIndexTable(), entry.getValue());
			if (result ==null || result.isEmpty()) continue;
			String key = result.get(condition.getRefTable().getKey());
			result = dbAdapter.getResult(condition.getRefTable().getTable(), key, fieldMap);
			if (result ==null || result.isEmpty()) continue;
			results.add(entry.getKey(), result);
		}
		
		return results;
	}

	@Override
	LinkedList<HashMap<String, String>> getAcResultList(
			AnnotationContext annotationContext) {
		LinkedList<HashMap<String, String>> resultList = results.get(annotationContext.getGeneName());
		return resultList;
	}

}
