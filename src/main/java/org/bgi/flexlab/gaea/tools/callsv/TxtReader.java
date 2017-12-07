package org.bgi.flexlab.gaea.tools.callsv;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * TxtReader类，用于读取程序的中间文件
 * @author Huifang Lu
 *
 */
public class TxtReader {
	
	private Configuration conf;
	
	/**
	 * 带有参数的构造函数
	 * @param conf Configuration类的conf
	 */
	public TxtReader(Configuration conf) {
		this.conf = conf;
	}
	
	/**
	 * readFile方法，用于读取中间文件
	 * @param libconftxt 输入的参数是一个HFDFS地址，一般是此程序的中间文件，保存了insert size或者dist
	 * @return 一个Map类型的集合，保存了每一个文库或者染色体相对应的insert size或者dist值
	 */
	
	public Map<String, List<Integer>> readFile(String libconftxt){
		
		Map<String, List<Integer>> map = new TreeMap<String, List<Integer>>();
		
		BufferedReader br = null;
		FileSystem fs;
		
		try {
			fs = FileSystem.get(this.conf);
			FileStatus[] flist = fs.listStatus(new Path(libconftxt));
			
			for(FileStatus file : flist) {
				
				FSDataInputStream fsopen = fs.open(file.getPath());
				br = new BufferedReader(new InputStreamReader(fsopen));
				
				String line = null;
				while((line = br.readLine())!= null) {
					String[] lines = line.split("\\t");
					
					List<Integer> dataList = map.get(lines[0]);
					if(dataList==null)
						dataList = new ArrayList<Integer>();
					
					for(int i=1; i<lines.length; i++) {
						try {
							dataList.add((int)Float.parseFloat(lines[i]));
						}catch(Exception e) {
							continue;
						}
					}
					map.put(lines[0], dataList);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		return map;
	}
	
	
	public Map<String, List<Integer>> readConfFile(String libconftxt, String chr){
		Map<String, List<Integer>> map = new TreeMap<String, List<Integer>>();
		
		BufferedReader br = null;
		FileSystem fs;
		
		try {
			fs = FileSystem.get(this.conf);
			FileStatus[] flist = fs.listStatus(new Path(libconftxt));
			
			for(FileStatus file : flist) {
				
				FSDataInputStream fsopen = fs.open(file.getPath());
				br = new BufferedReader(new InputStreamReader(fsopen));
				
				String line = null;
				while((line = br.readLine())!= null) {
					String[] lines = line.split("\\t");
					
					if(!(lines[1].equals(chr)))
						continue;
					
					List<Integer> dataList = map.get(lines[0]);
					if(dataList==null)
						dataList = new ArrayList<Integer>();
					
					for(int i=2; i<lines.length; i++) {
						try {
							dataList.add((int)Float.parseFloat(lines[i]));
						}catch(Exception e) {
							continue;
						}
					}
					map.put(lines[0], dataList);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		return map;
	}

}
