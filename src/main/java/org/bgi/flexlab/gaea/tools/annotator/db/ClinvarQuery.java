package org.bgi.flexlab.gaea.tools.annotator.db;

import java.util.HashMap;

import org.bgi.flexlab.gaea.tools.annotator.config.Config;
import org.bgi.flexlab.gaea.tools.annotator.db.Condition.ConditionKey;

public class ClinvarQuery extends AbstractDBQuery {

	private static final long serialVersionUID = -5315986884566094464L;
	
	@Override
	public boolean check(HashMap<ConditionKey, String> certainValue,
			HashMap<String, String> resultMap) {
		String alt = resultMap.get("AlternateAllele");
		String ref = Config.get().getRef();
		
		if (resultMap.get("Assembly").equals(ref) && 
				certainValue.get(ConditionKey.ALT).equalsIgnoreCase(alt)){
			return true;
		}
		return false;
	}

}
