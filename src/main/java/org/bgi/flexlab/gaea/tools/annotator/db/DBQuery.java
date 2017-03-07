package org.bgi.flexlab.gaea.tools.annotator.db;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.bgi.flexlab.gaea.tools.annotator.config.DatabaseInfo.DbType;
import org.bgi.flexlab.gaea.tools.annotator.effect.AnnotationContext;

/**
 * @author huangzhibo
 *
 */
public class DBQuery implements Serializable {

	private static final long serialVersionUID = -897843908487603204L;
	
	DBAdapterInterface dbAdapter = null;
	Results results = null;
	Condition condition = null;
	
	/**
	 * 执行query并判断结果，通过getResults方法获取结果
	 * @param condition
	 * @return
	 * @throws IOException
	 */
	boolean executeQuery(Condition condition) throws IOException {
		this.condition = condition;
		Results results = query(condition);
		
		if (results == null || results.isEmpty()) {
			return false;
		}else {
			this.results = results;
			return true;
		}
	}
	
	/**
	 * 根据condition查询数据库
	 * @param condition
	 * @return
	 * @throws IOException
	 */
	public Results query(Condition condition)throws IOException{
		HashMap<String, String> fieldMap = condition.getFields();
		Results results = new Results();

		HashMap<String,String> result = dbAdapter.getResult(condition.getRefTable().getIndexTable(), condition.getConditionString());
		if (result ==null || result.isEmpty()) return null;
		List<String> alts = condition.getAlts();
		String keyStr = result.get(condition.getRefTable().getKey());
		String[] keys = keyStr.split(",");
		for (String key : keys) {
			result = dbAdapter.getResult(condition.getRefTable().getTable(), key);
			
			HashMap<String,String> annoResult = new HashMap<String, String>();
			for (Entry<String, String> entry : fieldMap.entrySet()) {
				annoResult.put(entry.getKey(), result.get(entry.getValue()));
			}
			
			if (result ==null || result.isEmpty()){
				System.err.println("Cann't find value from table:"+condition.getRefTable().getTable()+". Key:"+key);
				return null;
			}
//			result.put(condition.getRefTable().getKey(), key);
			String altStr = result.get("ALT");
			if (altStr == null) {
				System.err.println("Alt is null:"+condition.getRefTable().getTable()+". Key:"+key);
				return null;
			}
			
			if (altStr.indexOf(",") == -1) {
				altStr = altStr.toUpperCase();
				if(alts.contains(altStr)){
					results.add(altStr, annoResult);
				}
			}else {
				String[] alt_list = altStr.split(",");
//				splitResult();
				for (String alt : alt_list) {
					alt = alt.toUpperCase();
					if(alts.contains(alt)){
						results.add(alt, annoResult);
					}
				}
			}
		}
		
		return results;
	}
	
	/**
	 * 对含多个变异的结果进行分割
	 * @param result
	 * @return LinkedList<HashMap<String, String>>
	 */
	private LinkedList<HashMap<String, String>> splitResult(HashMap<String, String> result, String altStr) {
		LinkedList<HashMap<String, String>> resultList = new LinkedList<HashMap<String, String>>();
	    //String[] alt_list = altStr.split(",");
		//HashMap<String, String> r = (HashMap<String, String>) result.clone();
		for (Entry<String, String> entry : result.entrySet()) {
			if (entry.getValue().indexOf(",")!=-1) {
				entry.getValue().split(",");
			}
		}
		return resultList;
	}
	
	/**
	 * 从results获取符合annotationContext的结果
	 * @param annotationContext
	 * @return
	 */
	LinkedList<HashMap<String, String>> getAcResultList(AnnotationContext ac) {
		LinkedList<HashMap<String, String>> resultList = results.get(ac.getAllele());
		return resultList;
	}
	
	/**
	 * 对查询结果results进行矫正
	 * @param results
	 */
	//abstract void adjustResult(HashMap<String,String> result);
	
	Results getResults(){
		return results;
	}

	public void disconnection() throws IOException {
		dbAdapter.disconnection();
	}

	public void connection(String dbName, DbType dbType, String connInfo) throws IOException{
		dbAdapter = DBAdapterFactory.createDbAdapter(dbType, connInfo);
		dbAdapter.connection(dbName);
	}
	
}
