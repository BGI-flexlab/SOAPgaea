package org.bgi.flexlab.gaea.tools.annotator.db;

import java.util.HashMap;

import org.bgi.flexlab.gaea.tools.annotator.db.Condition.ConditionKey;

public class G1000Query extends DBsnpQuery {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6394064009406746604L;
	
	@Override
	public boolean check(HashMap<ConditionKey, String> certainValue,
			HashMap<String, String> resultMap) {
		String alt = resultMap.get("alt");
		if(certainValue.get(ConditionKey.ALT).equalsIgnoreCase(alt)){
			return true;
		}
		return false;
	}
}
