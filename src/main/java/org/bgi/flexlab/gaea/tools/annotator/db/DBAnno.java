package org.bgi.flexlab.gaea.tools.annotator.db;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

import org.bgi.flexlab.gaea.tools.annotator.config.Config;
import org.bgi.flexlab.gaea.tools.annotator.config.TableInfo;
import org.bgi.flexlab.gaea.tools.annotator.db.Condition.ConditionKey;
import org.bgi.flexlab.gaea.tools.annotator.effect.AnnotationContext;
import org.bgi.flexlab.gaea.tools.annotator.effect.VcfAnnotationContext;
import org.bgi.flexlab.gaea.tools.annotator.interval.Variant;

public class DBAnno implements Serializable{
	
	private static final long serialVersionUID = -3944211982294335404L;
	
	private Config config = null;
	HashMap<String, AbstractDbQuery> DbQueryMap = null;
	
	public DBAnno(Config config){
		this.config = config;
	}
	
	public void annotate(VcfAnnotationContext vcfAnnoContext) {
		// TODO Auto-generated method stub
		
		List<String> alts = vcfAnnoContext.getAlts();
		for (String alt : alts) {
			AnnotationContext annoContext = vcfAnnoContext.getAnnotationContext(alt);
			Variant variant = vcfAnnoContext.getVariant(alt);
			if (variant != null) {
				System.err.println("[TEST] Varint is null in DBAnno.");
			}
			variantAnno(annoContext, variant);
		}
	}
	
	public HashMap<String, String> variantAnno(AnnotationContext annoContext, Variant variant) {
		
		AbstractDbQuery dbQuery = null;
		LinkedHashMap<ConditionKey, String> conditionMap = new LinkedHashMap<ConditionKey, String>();
		
		conditionMap.put(ConditionKey.CHR, variant.getChromosomeName());
		conditionMap.put(ConditionKey.POS, String.valueOf(variant.getStart()));
		conditionMap.put(ConditionKey.END, String.valueOf(variant.getEnd()));
		conditionMap.put(ConditionKey.ALT, String.valueOf(variant.getAlt()));
		
		
		HashMap<String, String[]> tagsByDB = config.getTagsByDB();
		HashMap<String, TableInfo> dbInfo = config.getDbInfo();
		for (String dbName : tagsByDB.keySet()) {
			
			if(DbQueryMap.containsKey(dbName)){
				dbQuery = DbQueryMap.get(dbName);
			}else {
				try{
					dbQuery = (AbstractDbQuery)Class.forName("org.bgi.flexlab.gaea.annotator.db." + dbInfo.get(dbName).getQueryClassName()).newInstance();
		        }catch(Exception e){
		            e.printStackTrace();
		        }
				DbQueryMap.put(dbName, dbQuery);
			}
			
			TableInfo tableInfo = dbInfo.get(dbName);
			String[] tags = tagsByDB.get(dbName);
			String[] fields = toFields(tableInfo.getFields(), tags);
			Condition condition = new Condition(dbName,tableInfo,conditionMap);
			
			HashMap<String,String> annoResults= dbQuery.query(condition, fields);
			for (String annoTag : tags) {
				annoContext.putAnnoItem(annoTag, annoResults.get(annoTag));
			}
		}
		return null;
	}

	private String[] toFields(HashMap<String, String> fields, String[] fieldList) {
		String[] tags = new String[fieldList.length];
		for (int i = 0; i < fieldList.length; i++) {
			tags[i] = fields.get(fieldList[i]);
		}
		return tags;
	}

}
