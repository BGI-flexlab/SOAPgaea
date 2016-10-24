package org.bgi.flexlab.gaea.tools.annotator.config;

import java.io.Serializable;
import java.util.HashMap;

public class DatabaseInfo implements Serializable {

	public enum DbType {
		HBASE
		, MYSQL
	}
	
	private static final long serialVersionUID = -5850759043237677551L;
	
	private final DbType database;
	private final String queryClassName;
	private final String queryCondition;
	private final RefTableInfo GRCh37; //HBASE: table,key,indexTable
	private final RefTableInfo GRCh38;
	private final HashMap<String, String> fields;
	
	public DatabaseInfo(DbType database,String queryCondition, String queryClassName, RefTableInfo GRCh37, RefTableInfo GRCh38, HashMap<String, String> fields) {
		this.database = database;
		this.queryClassName = queryClassName;
		this.queryCondition = queryCondition;
		this.fields = fields;
		this.GRCh37 = GRCh37;
		this.GRCh38 = GRCh38;
	}

	public HashMap<String, String> getFields() {
		return fields;
	}

	public RefTableInfo getRefTable(String ref) {
		if (ref.equals(GRCh38)) {
			return GRCh38;
		}else {
			return GRCh37;
		}
	}

	public String getQueryCondition() {
		return queryCondition;
	}

	public String getQueryClassName() {
		return queryClassName;
	}

	public DbType getDatabase() {
		return database;
	}

}
