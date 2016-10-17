package org.bgi.flexlab.gaea.tools.annotator.db;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import org.bgi.flexlab.gaea.tools.annotator.config.Config;
import org.bgi.flexlab.gaea.tools.annotator.config.TableInfo;
import org.bgi.flexlab.gaea.tools.annotator.db.Condition.ConditionKey;
import org.bgi.flexlab.gaea.tools.annotator.effect.AnnotationContext;
import org.bgi.flexlab.gaea.tools.annotator.effect.VcfAnnotationContext;

public class DBAnnotator implements Serializable{
	
	private static final long serialVersionUID = -3944211982294335404L;
	private static HashMap<String, AbstractDBQuery> DbQueryMap = new HashMap<String, AbstractDBQuery>();
	
	private Config config;
	
	
	public DBAnnotator(Config config){
		this.config = config;
	}
	
	public void annotate(VcfAnnotationContext vac) throws IOException {
		LinkedHashMap<ConditionKey, String> conditionMap = new LinkedHashMap<ConditionKey, String>();
		
		conditionMap.put(ConditionKey.CHR, vac.getChrome());
		conditionMap.put(ConditionKey.POS, String.valueOf(vac.getStart()));

		List<String> dbNameList = config.getDbNameList();
		for (String dbName : dbNameList) {
			List<AnnotationContext> list = vac.getAnnotationContexts();
			TableInfo tableInfo = config.getDbInfo().get(dbName);
			AbstractDBQuery dbQuery = DbQueryMap.get(dbName);
			
			for (AnnotationContext annotationContext : list) {
				conditionMap.put(ConditionKey.GENE, annotationContext.getGeneName());
				conditionMap.put(ConditionKey.END, String.valueOf(vac.getStart() + annotationContext.getAllele().length() - 1));
				conditionMap.put(ConditionKey.ALT, String.valueOf(annotationContext.getAllele()));
					
				String[] fields = config.getFieldsByDB(dbName);
				Condition condition = new Condition(dbName,tableInfo,conditionMap);
				HashMap<String,String> annoResults= dbQuery.query(condition, fields);
				if (annoResults == null || annoResults.isEmpty()) continue;
				for (String field : fields) {
					annotationContext.putAnnoItem(field, annoResults.get(field));
				}
			}
		}
	}

	public void connection() throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException {
		List<String> dbNameList = config.getDbNameList();
		for (String dbName : dbNameList) {
			TableInfo tableInfo = config.getDbInfo().get(dbName);
			AbstractDBQuery dbQuery = (AbstractDBQuery)Class.forName("org.bgi.flexlab.gaea.tools.annotator.db." + tableInfo.getQueryClassName()).newInstance();
			dbQuery.connection(dbName, tableInfo.getDatabaseType());
			DbQueryMap.put(dbName, dbQuery);
		}
	}

	public void disconnection() throws IOException {
		Set<String> keys = DbQueryMap.keySet();
		for (String key : keys) {
			AbstractDBQuery dbQuery = DbQueryMap.get(key);
			if (dbQuery != null) {
				dbQuery.disconnection();
			}
		}
	}

}
