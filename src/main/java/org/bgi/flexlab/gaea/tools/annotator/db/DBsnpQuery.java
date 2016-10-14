package org.bgi.flexlab.gaea.tools.annotator.db;

import java.util.HashMap;

import org.bgi.flexlab.gaea.tools.annotator.db.Condition.ConditionKey;

public class DBsnpQuery extends AbstractDBQuery {

	private static final long serialVersionUID = -9081062168317341102L;

	@Override
	public boolean check(HashMap<ConditionKey, String> certainValue,
			HashMap<String, String> resultMap) {
		String alt = resultMap.get("ALT");
		if(certainValue.get(ConditionKey.ALT).equalsIgnoreCase(alt)){
			String infoStr = resultMap.get("INFO");
			String[] infos;
			if(infoStr.indexOf(';')>=0) {
				StringBuilder attr = new StringBuilder();
				infos = infoStr.split(";");
				for (int i = 0; i < infos.length; i++) {
					if (infos[i].indexOf('=')>=0) {
						String[] keyValue = infos[i].split("=");
						resultMap.put(keyValue[0], keyValue[1]);
					}else {
						attr.append(infos[i]);
					}
				}
				resultMap.put("attr", attr.toString());
			}
			
			return true;
		}
		return false;
	}

}
