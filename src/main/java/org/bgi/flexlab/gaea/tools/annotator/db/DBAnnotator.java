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

import org.bgi.flexlab.gaea.tools.annotator.config.Config;
import org.bgi.flexlab.gaea.tools.annotator.config.DatabaseInfo;
import org.bgi.flexlab.gaea.tools.annotator.config.DatabaseInfo.DbType;
import org.bgi.flexlab.gaea.tools.annotator.effect.AnnotationContext;
import org.bgi.flexlab.gaea.tools.annotator.effect.VcfAnnotationContext;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

public class DBAnnotator implements Serializable{
	
	private static final long serialVersionUID = -3944211982294335404L;
	private static HashMap<String, DBQuery> DbQueryMap = new HashMap<String, DBQuery>();
	
	private Config config;
	private HashMap<String, Condition> dbConditionHashMap;
	
	public DBAnnotator(Config config){
		this.config = config;
		dbConditionHashMap = new HashMap<String, Condition>();
	}
	
	public void annotate(VcfAnnotationContext vac) throws IOException {
		
		List<String> dbNameList = config.getDbNameList();
		for (String dbName : dbNameList) {
			Condition condition = dbConditionHashMap.get(dbName);
			condition.createConditionMap(vac);
			
			DBQuery dbQuery = DbQueryMap.get(dbName);
			if (!dbQuery.executeQuery(condition)) continue;
			
			for (AnnotationContext annotationContext : vac.getAnnotationContexts()) {
				LinkedList<HashMap<String,String>> resultList = dbQuery.getAcResultList(annotationContext);
				HashMap<String,String> result = mergeResult(resultList);
				if (result == null) continue;
				for (Entry<String, String> entry : result.entrySet()) {
					annotationContext.putAnnoItem(entry.getKey(),entry.getValue(),false);
				}
			}
			
		}
	}
	public void annotate(List<VcfAnnotationContext> vacs) throws IOException {

		List<String> dbNameList = config.getDbNameList();
		if(vacs == null)
			return;
		VcfAnnotationContext vac0 = vacs.get(0);
		for (String dbName : dbNameList) {
			Condition condition = dbConditionHashMap.get(dbName);
			condition.createConditionMap(vac0);

			DBQuery dbQuery = DbQueryMap.get(dbName);
			if (!dbQuery.executeQuery(condition)) continue;

			for(VcfAnnotationContext vac : vacs) {
				for (AnnotationContext annotationContext : vac.getAnnotationContexts()) {
					LinkedList<HashMap<String, String>> resultList = dbQuery.getAcResultList(annotationContext);
					HashMap<String, String> result = mergeResult(resultList);
					if (result == null) continue;
					for (Entry<String, String> entry : result.entrySet()) {
						annotationContext.putAnnoItem(entry.getKey(), entry.getValue(), false);
					}
				}
			}

		}
	}
	private HashMap<String, String> mergeResult(
			LinkedList<HashMap<String, String>> resultList) {
		if (resultList == null || resultList.isEmpty()) return null;
		
		if (resultList.size() == 1) {
			return resultList.get(0);
		}
		
		HashMap<String, String> result = resultList.get(0);
		resultList.removeFirst();
		for (HashMap<String, String> r : resultList) {
			for (Entry<String, String> entry : r.entrySet()) {
				if(!result.get(entry.getKey()).equals(entry.getValue())){
					String value = result.get(entry.getKey()) + "," + entry.getValue();
					result.put(entry.getKey(), value);
				}
			}
		}
		return result;
	}

	public void connection() throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException {
		List<String> dbNameList = config.getDbNameList();
		for (String dbName : dbNameList) {
			DatabaseInfo databaseInfo = config.getDatabaseJson().getDatabaseInfo(dbName);
			String queryClassName = "DBQuery";
			if (databaseInfo.getQueryClassName() != null) {
				queryClassName = databaseInfo.getQueryClassName();
				System.err.println("queryClassName is :" +queryClassName);
				System.err.println("queryCondition is :" +databaseInfo.getQueryCondition());
			}else {
				System.err.println("queryClassName is null, use DBQuery.class defaultly. This maybe a bug! dbName:" +dbName);
			}
			DBQuery dbQuery = (DBQuery)Class.forName("org.bgi.flexlab.gaea.tools.annotator.db." + queryClassName).newInstance();
			DbType dbType = databaseInfo.getDatabase();
			String connInfo = config.getDatabaseJson().getConnectionInfo(dbType);
			dbQuery.connection(dbName, dbType,connInfo);
			DbQueryMap.put(dbName, dbQuery);
			
			Condition condition = new Condition(dbName,databaseInfo);
			dbConditionHashMap.put(dbName, condition);
		}
	}

	public void disconnection() throws IOException {
		Set<String> keys = DbQueryMap.keySet();
		for (String key : keys) {
			DBQuery dbQuery = DbQueryMap.get(key);
			if (dbQuery != null) {
				dbQuery.disconnection();
			}
		}
	}

}
