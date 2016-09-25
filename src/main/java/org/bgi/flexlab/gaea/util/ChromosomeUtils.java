package org.bgi.flexlab.gaea.util;

public class ChromosomeUtils {
	
	public static String formatChrName(String chrName) {
		String formatChrName = chrName;
		if(!chrName.startsWith("chr")) {
			formatChrName = "chr" + chrName;
		}
		return formatChrName.toLowerCase();
	}
	
}
