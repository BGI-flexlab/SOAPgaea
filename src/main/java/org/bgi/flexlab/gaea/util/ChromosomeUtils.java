package org.bgi.flexlab.gaea.util;

public class ChromosomeUtils {
	
	public static String formatChrName(String chrName) {
		chrName = chrName.toLowerCase();
		if(!chrName.startsWith("chr")) {
			chrName = "chr" + chrName;
		}
		return chrName;
	}
	
}
