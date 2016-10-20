package org.bgi.flexlab.gaea.tools.annotator.db;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
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
public class HbaseAdapter implements DBAdapterInterface{
	
	public static final String DEFAULT_COLUMN_FAMILY = "data";
	
	static Configuration conf = null;
    static {
        conf = HBaseConfiguration.create();
        conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
        conf.addResource(new Path("/etc/hbase/conf/core-site.xml"));
    }
    static Connection conn; 
    
    @Override
	public void connection(String dbName) throws IOException {
		conn = ConnectionFactory.createConnection(conf);
	}
	
    @Override
	public void disconnection() throws IOException {
			conn.close();
	}

    @Override
	public HashMap<String, String> getResult(String tableName, String rowKey) throws IOException {
		HashMap<String,String> resultMap = new HashMap<String,String>();
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addFamily(Bytes.toBytes(DEFAULT_COLUMN_FAMILY));
		Table table = conn.getTable(TableName.valueOf(tableName));
		Result result = table.get(get);
		
		for  (Cell cell : result.rawCells()) {
			String key = Bytes.toString(CellUtil.cloneQualifier (cell));
			String value = Bytes.toString(CellUtil.cloneValue(cell));
			resultMap.put(key, value);
		}
		return resultMap;
	}

	@Override
	public HashMap<String, String> getResult(String tableName,
			String rowKey, Set<String> tags) throws IOException{
		HashMap<String,String> resultMap = getResult(tableName,rowKey);
		Table table = conn.getTable(TableName.valueOf(tableName));
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addFamily(Bytes.toBytes(DEFAULT_COLUMN_FAMILY));
		Result result = table.get(get);
		for (String key : tags) {
			byte[] value = result.getValue(Bytes.toBytes(DEFAULT_COLUMN_FAMILY), Bytes.toBytes(key));
			resultMap.put(key, Bytes.toString(value));
		}
		return resultMap;
	}


}
