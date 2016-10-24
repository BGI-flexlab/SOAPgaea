package org.bgi.flexlab.gaea.tools.annotator.config;

public class RefTableInfo {
	private final String table;
	private final String indexTable;
	private final String key;
	
	public RefTableInfo(String table, String indexTable, String key) {
		this.table = table;
		this.indexTable = indexTable;
		this.key = key;
	}

	public String getTable() {
		return table;
	}

	public String getIndexTable() {
		return indexTable;
	}

	public String getKey() {
		return key;
	}

}
