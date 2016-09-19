package org.bgi.flexlab.gaea.tools.annotator.db;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.bgi.flexlab.gaea.tools.annotator.conf.TableInfo;
import org.bgi.flexlab.gaea.tools.annotator.conf.TableInfo.DbType;

/**
 * @author huangzhibo
 *
 * 获取不同数据库的查询条件
 */
public class Condition implements Serializable{

	
	public static enum ConditionKey {
		CHR
		, POS
		, END
		, ALT
		, GENE
	}
	
	private static final long serialVersionUID = 5474372956720082769L;
	
	private String dbName = null;
	private String tableName = null;
//	private String indexTableName = null;
	private String conditionString = null;
	private String family = "data";
	private DbType dbType = null;
	private TableInfo tableInfo = null;
	
	private LinkedHashMap<ConditionKey, String> conditionMap = null;
	
	public static String config = null;
	
	public Condition(String tableName){
		setTableName(tableName);
	}
	
	public Condition(String dbName, String tableName){
		setDbName(dbName);
		setTableName(tableName);
	}
	
	public Condition(String dbName, TableInfo tableInfo){
		setDbName(dbName);
		this.tableInfo = tableInfo;
	}
	
	public Condition(String dbName, TableInfo tableInfo, LinkedHashMap<ConditionKey, String>conditionMap){
		setDbName(dbName);
		this.tableInfo = tableInfo;
		this.tableName = tableInfo.getTable();
		this.conditionMap = conditionMap;
		this.dbType = tableInfo.getDatabase();
	}


	/**
	 * condition string type
	 */
	public String getConditionString() {
		StringBuilder sb = new StringBuilder();
		String[] conKeys = tableInfo.getQueryCondition().split("_");
		if(dbType == DbType.HBASE){
			for (int i = 0; i < conKeys.length-1; i++) {
				sb.append(conditionMap.get(conKeys[i])+'-');
			}
			sb.append(conKeys[conKeys.length-1]);
		}else {
			sb.append("where ");
			for (int i = 0; i < conKeys.length-1; i++) {
				sb.append(conKeys[i] + " = " + conditionMap.get(conKeys[i])+" and ");
			}
			sb.append(conKeys[conKeys.length-1] + " = " + conditionMap.get(conKeys[conKeys.length-1]));
		}
		return conditionString;
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getFamily() {
		return family;
	}

	public void setFamily(String family) {
		this.family = family;
	}

	public DbType getDbType() {
		return dbType;
	}

	public void setDbType(DbType dbType) {
		this.dbType = dbType;
	}

	public TableInfo getTableInfo() {
		return tableInfo;
	}

	public void setTableInfo(TableInfo tableInfo) {
		this.tableInfo = tableInfo;
	}

	public HashMap<ConditionKey, String> getConditionMap() {
		return conditionMap;
	}
	
}
