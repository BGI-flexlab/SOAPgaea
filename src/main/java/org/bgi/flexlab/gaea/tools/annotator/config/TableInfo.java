package org.bgi.flexlab.gaea.tools.annotator.config;

import java.io.Serializable;
import java.util.HashMap;

public class TableInfo implements Serializable {

	public enum DbType {
		HBASE
		, MYSQL
	}
	
	public enum QueryCondition {
		CHR_POS
		, CHR_POS_END
		, GENE
	}
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -5850759043237677551L;
	private final DbType database;
	private final String table;
	private final String key;
	private final String indexTable;
	private final String queryCondition;
	private final String queryClassName;
	private HashMap<String, String> fields;
	
	public TableInfo(DbType database,String table, String key, String queryCondition, String queryClassName, String indexTable, HashMap<String, String> fields) {
		this.database = database;
		this.table = table;
		this.key = key;
		this.queryCondition = queryCondition;
		this.queryClassName = queryClassName;
		this.indexTable = indexTable;
		this.fields = fields;
	}
	

	public HashMap<String, String> getFields() {
		return fields;
	}


	public String getKey() {
		return key;
	}


	public String getQueryCondition() {
		return queryCondition;
	}


	public String getIndexTable() {
		return indexTable;
	}


	public String getTable() {
		return table;
	}


	public DbType getDatabase() {
		return database;
	}


	public String getQueryClassName() {
		return queryClassName;
	}



}
