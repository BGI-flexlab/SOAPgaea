package org.bgi.flexlab.gaea.tools.annotator.db;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author huangzhibo
 *
 */
public class HbaseAdapter implements DBAdapterInterface {
	
	static Configuration conf = null;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost");
    }
    static Connection conn; 
    
	@Override
	public void connection(){
		try {
			conn = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	@Override
	public void connection(String dbName) {
		connection();
	}

	@Override
	public void disconnection() throws IOException {
			conn.close();
	}

	 /*
     * 根据rwokey查询
     * 
     * @rowKey rowKey
     * 
     * @tableName 表名
     */
	@Override
	public HashMap<String, String> getResult(String stableName, String condition, String[] tags) {
		HashMap<String,String> resultMap = new HashMap<String,String>();
		Result result = null;
		try {
			Get get = new Get(Bytes.toBytes(condition));
			Table table = conn.getTable(TableName.valueOf(stableName));
			result = table.get(get);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		for (String key : tags) {
			byte[] value = result.getValue(Bytes.toBytes("data"), Bytes.toBytes(key));
			resultMap.put(key, value.toString());
		}
		
//		resultMap.put(condition, result.getRow().toString());
		return resultMap;
	}

//	TODO 
//	public HashMap<String, String> getResult(String stableName, String condition) {
//		HashMap<String,String> resultMap = new HashMap<String,String>();
//		Result result = null;
//		try {
//			Get get = new Get(Bytes.toBytes(condition));
//			Table table = conn.getTable(TableName.valueOf(stableName));
//			result = table.get(get);
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		
//		for (String key : tags) {
//			byte[] value = result.getValue(Bytes.toBytes("data"), Bytes.toBytes(key));
//			resultMap.put(key, value.toString());
//		}
//		
////		resultMap.put(condition, result.getRow().toString());
//		return resultMap;
//	}


}
