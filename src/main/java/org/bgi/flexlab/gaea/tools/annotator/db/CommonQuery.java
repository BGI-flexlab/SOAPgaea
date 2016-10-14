package org.bgi.flexlab.gaea.tools.annotator.db;

import java.util.HashMap;

import org.bgi.flexlab.gaea.tools.annotator.db.Condition.ConditionKey;

public class CommonQuery extends AbstractDBQuery {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7515609026719464241L;


	public boolean check(HashMap<ConditionKey, String> certainValue,
			HashMap<String, String> resultMap) {
		
		if (resultMap.containsKey("alt")) {
			String alt = resultMap.get("alt");
			if(certainValue.get(ConditionKey.ALT).equalsIgnoreCase(alt)){
				return true;
			}
		}else if (resultMap.containsKey("ALT")) {
			String alt = resultMap.get("ALT");
			if(certainValue.get(ConditionKey.ALT).equalsIgnoreCase(alt)){
				return true;
			}
			
		}
		return true;
	}

}
