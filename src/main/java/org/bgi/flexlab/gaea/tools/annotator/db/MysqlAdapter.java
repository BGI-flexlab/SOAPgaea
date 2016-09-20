package org.bgi.flexlab.gaea.tools.annotator.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;


public class MysqlAdapter implements DBAdapterInterface {
	
	
	private static final String USER="root";
	private static final String PASSWORD="";
	
	private static String driver ="com.mysql.jdbc.Driver";
	private static String url=null;
	private static Connection conn=null;
	
	MysqlAdapter(){
		
	}
	MysqlAdapter(String url){
		MysqlAdapter.url = url;
	}
	
	MysqlAdapter(String driver, String url){
		MysqlAdapter.setDriver(driver);
		MysqlAdapter.url = url;
	}
	
	@Override
	public void connection() {
		try {
			//1.加载驱动程序
			Class.forName(driver);
			//2.获得数据库的连接
			setConn(DriverManager.getConnection(url, USER, PASSWORD));
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void connection(String dbName) {
//		url = url
		try {
			//1.加载驱动程序
			Class.forName("com.mysql.jdbc.Driver");
			//2.获得数据库的连接
			setConn(DriverManager.getConnection(url+dbName, USER, PASSWORD));
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private void setConn(Connection connection) {
		conn = connection;
	}
	
	public static Connection getConn() {
		return conn;
	}

	@Override
	public void disconnection() {
		try {
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static String getDriver() {
		return driver;
	}
	
	public static void setDriver(String driver) {
		MysqlAdapter.driver = driver;
	}
	
	@Override
	public HashMap<String, String> getResult(String stableName, String condition, String[] tags) {
		HashMap<String,String> resultMap = new HashMap<String,String>();
		StringBuilder sb=new StringBuilder();
		sb.append("select * from " + stableName + " ");
		sb.append(condition);
		PreparedStatement ptmt = null;
		ResultSet rs = null;
		try {
			ptmt=conn.prepareStatement(sb.toString());
			rs=ptmt.executeQuery();
			for (String key : tags) {
					resultMap.put(key, rs.getString(key));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return resultMap;
	}


}
