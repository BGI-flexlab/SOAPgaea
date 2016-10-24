package org.bgi.flexlab.gaea.tools.annotator.config;

import java.io.Serializable;
import java.util.HashMap;

import org.bgi.flexlab.gaea.tools.annotator.config.DatabaseInfo.DbType;

public class DatabaseJson implements Serializable{

	private static final long serialVersionUID = -3360762372473610852L;
	
	private final HashMap<DbType, String> connectionInfo;
	private final HashMap<String, DatabaseInfo> databaseInfo;

	public DatabaseJson(HashMap<DbType, String> connectionInfo, HashMap<String, DatabaseInfo> databaseInfo) {
		this.connectionInfo = connectionInfo;
		this.databaseInfo = databaseInfo;
	}

	public String getConnectionInfo(DbType connString) {
		return connectionInfo.get(connString);
	}

	public DatabaseInfo getDatabaseInfo(String dbName) {
		return databaseInfo.get(dbName);
	}

}
