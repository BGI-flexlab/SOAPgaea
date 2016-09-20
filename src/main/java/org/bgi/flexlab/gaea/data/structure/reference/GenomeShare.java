/**
 * Copyright (c) 2011, BGI and/or its affiliates. All rights reserved.
 * BGI PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package org.bgi.flexlab.gaea.data.structure.reference;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

/**
 * Genome内存共享
 * @author ZhangZhi && ChuanJun && ZhangYong
 *
 */
public class GenomeShare {
	
	/**
	 * 染色体信息Map
	 */
	private Map<String, ChromosomeInfoShare> chromosomeInfoMap = new ConcurrentHashMap<String, ChromosomeInfoShare>();	
	
	/**
	 * boolean for distribute reference
	 */
	private  boolean isdistributeRef = false;
	
	/**
	 * distribute cache reference
	 * @param chrList
	 * @param conf
	 * @return
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	public static boolean disCacheRef(String chrList, Configuration conf) throws IOException, URISyntaxException {
		DistributedCache.createSymlink(conf);
		DistributedCache.addCacheFile(new URI(chrList+ "#" + "refList"), conf);
		
		Path refPath = new Path(chrList);
		FileSystem fs = refPath.getFileSystem(conf);
		FSDataInputStream refin = fs.open(refPath);
		LineReader in = new LineReader(refin);
		Text line = new Text();
		
		String chrFile = "";
		String[] chrs = new String[3];
		while((in.readLine(line)) != 0){
			chrFile = line.toString();
			chrs = chrFile.split("\t");
			File fileTest = new File(chrs[1]);
			if(fileTest.isFile()) {
				chrs[1] = "file://" + chrs[1];
			}
			DistributedCache.addCacheFile(new URI(chrs[1] + "#" + chrs[0] + "ref"), conf);
		}
		in.close();
		refin.close();
		System.out.println("> Distributed cached reference done.");
		return true;
	}
	
	/**
	 * do distribute
	 * @param chrList
	 * @param dbSNPList
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	public static void GenomeDistributeCache(String chrList, String dbSNPList) throws IOException, URISyntaxException {
		Configuration conf = new Configuration();
		
		//分布式缓存ref
        if(disCacheRef(chrList, conf)) {
        	conf.setBoolean("cacheref", true);
        } else {
        	System.err.println("Error distribute cache reference!");
        	System.exit(1);
        }
	}
	
	public boolean loadGenome(String refList, String dbsnpList) {
		try {
			loadChrList(refList);
		} catch (Exception e) {
			// TODO: handle exception
			return false;
		}
		return true;
	}
	
	public boolean loadGenome(Configuration conf) {
		isdistributeRef = conf.getBoolean("cacheref", false);
		if(!isdistributeRef)
			return false;
		
		try {
			loadChrList();
		} catch (Exception e) {
			// TODO: handle exception
			return false;
		}
		return true;
	}
	
	/**
	 * 循环调用loadChr方法，分别映射染色体文件，获得染色体长度
	 * @param chrList	包含所有染色体文件路径的list文件
	 * @throws IOException
	 */
	public void loadChrList(String chrList) throws IOException{
		Configuration conf = new Configuration();
		Path refPath = new Path(chrList);
		FileSystem fs = refPath.getFileSystem(conf);
		FSDataInputStream refin = fs.open(refPath);
		LineReader in = new LineReader(refin);
		Text line = new Text();
		
		String chrFile = "";
		String[] chrs = new String[3];
		while((in.readLine(line)) != 0){
			chrFile = line.toString();
			chrs = chrFile.split("\t");
			// insert chr
			if(!addChr(chrs[0])) {
				StringBuilder errorDescription = new StringBuilder();
				errorDescription.append("map Chromosome ");
				errorDescription.append(chrs[1]);
				errorDescription.append(" Failed.");
				System.err.println(errorDescription.toString());
			}
			if (chromosomeInfoMap.containsKey(chrs[0])) {
				// map chr and get length
				chromosomeInfoMap.get(chrs[0]).loadChr(chrs[1]);
				chromosomeInfoMap.get(chrs[0]).setLength(Integer.parseInt(chrs[2]));
				chromosomeInfoMap.get(chrs[0]).setChrName(chrs[0]);
			}
		}
		in.close();
		refin.close();
	}
	
	/**
	 * 循环调用loadChr方法，分别映射染色体文件，获得染色体长度
	 * 从distribute cache中读取
	 * @throws IOException
	 */
	public void loadChrList() throws IOException{
		BufferedReader br = new BufferedReader(new FileReader(new File("refList")));
		
		String line = new String();
		while((line = br.readLine()) != null) {
			String[] chrs = line.split("\t");
			System.err.println(line);
			// insert chr
			if(!addChr(chrs[0])) {
				StringBuilder errorDescription = new StringBuilder();
				errorDescription.append("map Chromosome ");
				errorDescription.append(chrs[1]);
				errorDescription.append(" Failed.");
				System.err.println(errorDescription.toString());
			}
			if (chromosomeInfoMap.containsKey(chrs[0])) {
				// map chr and get length
				chromosomeInfoMap.get(chrs[0]).loadChr(chrs[0] + "ref");
				chromosomeInfoMap.get(chrs[0]).setLength(Integer.parseInt(chrs[2]));
				chromosomeInfoMap.get(chrs[0]).setChrName(chrs[0]);
			}
		}
		br.close();
	}
	
	/**
	 * 循环调用loadChr方法，分别映射染色体文件，获得染色体长度
	 * @param chrList	包含所有染色体文件路径的list文件
	 * @throws IOException
	 */
	public void loadBasicInfo(String chrList) throws IOException{
		Configuration conf = new Configuration();
		Path refPath = new Path(chrList);
		FileSystem fs = refPath.getFileSystem(conf);
		FSDataInputStream refin = fs.open(refPath);
		LineReader in = new LineReader(refin);
		Text line = new Text();
		
		String chrFile = "";
		String[] chrs = new String[3];
		while((in.readLine(line)) != 0){
			chrFile = line.toString();
			chrs = chrFile.split("\t");
			
			// insert chr
			if(!addChr(chrs[0])) {
				StringBuilder errorDescription = new StringBuilder();
				errorDescription.append("map Chromosome ");
				errorDescription.append(chrs[1]);
				errorDescription.append(" Failed.");
				System.err.println(errorDescription.toString());
			}
			if (chromosomeInfoMap.containsKey(chrs[0])) {
				// map chr and get length
				chromosomeInfoMap.get(chrs[0]).setLength(Integer.parseInt(chrs[2]));
				chromosomeInfoMap.get(chrs[0]).setChrName(chrs[0]);
			}
		}
		in.close();
		refin.close();
	}
	
	/**
	 * 循环调用loaddbSNP方法，分别映射dbsnp文件
	 * @param dbsnpListPath	包含所有dbsnp文件路径的list文件
	 * @throws IOException
	 */
	
	public void loaddbSNPList(String dbsnpListPath) throws IOException{
		Configuration conf = new Configuration();
		Path dbPath = new Path(dbsnpListPath);
		FileSystem fs = dbPath.getFileSystem(conf);
		FSDataInputStream dbsnpin = fs.open(dbPath);
		LineReader in = new LineReader(dbsnpin);
		Text line = new Text();	
		
		String listLine = null;
		String[] listSplit = null;
		String chromosome = null;
		String dbsnpPath = null;
		String indexPath = null;
		int size = 0;
		while((in.readLine(line)) != 0){
			listLine = line.toString();
			listSplit = listLine.split("\t");
			chromosome = listSplit[0];
			dbsnpPath = listSplit[1];
			indexPath = listSplit[2];
			size = Integer.parseInt(listSplit[3]);
			if (chromosomeInfoMap.containsKey(chromosome)) {
				chromosomeInfoMap.get(chromosome).loaddbSNP(dbsnpPath, indexPath, size);
			}
		}
		in.close();
		dbsnpin.close();
	}	
	
	/**
	 * 循环调用loaddbSNP方法，分别映射dbsnp文件
	 * 从distribute cache读取
	 * @throws IOException
	 */
	
	public void loaddbSNPList() throws IOException{
		BufferedReader br = new BufferedReader(new FileReader(new File("dbsnpList")));
		
		String line = new String();
		String chromosome = null;
		String dbsnpPath = null;
		String indexPath = null;
		int size = 0;
		while((line = br.readLine()) != null) {
			String[] listSplit = line.split("\t");
			chromosome = listSplit[0];
			dbsnpPath = chromosome + "dbsnp";
			indexPath = chromosome + "dbsnpIndex";
			size = Integer.parseInt(listSplit[3]);
			if (chromosomeInfoMap.containsKey(chromosome)) {
				chromosomeInfoMap.get(chromosome).loaddbSNP(dbsnpPath, indexPath, size);
			}
		}
		br.close();
	}
	
	/**
	 * format chrname
	 */
	private static String getChrnameFormat(String chrName) {
		String newChrName = chrName.toLowerCase();
		if(!chrName.startsWith("chr")) {
			newChrName = "chr" + newChrName;
		}
		return newChrName;
	}
	
	
	/**
	 * 获取染色体信息Map
	 * @return
	 */
	public Map<String, ChromosomeInfoShare> getChromosomeInfoMap() {
		return chromosomeInfoMap;
	}

	/**
	 * 根据名称获取染色体信息
	 * @param chrName
	 * @return
	 */
	public ChromosomeInfoShare getChromosomeInfo(String chrName) {
		if(chromosomeInfoMap.get(getChrnameFormat(chrName)) == null)
			throw new RuntimeException("chr name not in GaeaIndex of ref:" + chrName);
		return chromosomeInfoMap.get(getChrnameFormat(chrName));
	}

	/**
	 * 添加染色体信息
	 * @param chrName 染色体名称
	 * @return
	 */
	public boolean addChr(String chrName){		
		ChromosomeInfoShare newChr = new ChromosomeInfoShare();
		chromosomeInfoMap.put(chrName, newChr);
		if(chromosomeInfoMap.get(chrName) != null) {
			return true;
		} else {
			return false;
		}
	}
}
