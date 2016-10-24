/**
 * Copyright (c) 2011, BGI and/or its affiliates. All rights reserved.
 * BGI PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package org.bgi.flexlab.gaea.data.structure.reference;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.bgi.flexlab.gaea.util.ChromosomeUtils;

/**
 * Genome内存共享
 * @author ZhangZhi && ChuanJun && ZhangYong
 *
 */
public class GenomeShare {
	
	/**
	 * 染色体信息Map
	 */
	private Map<String, ChromosomeInformationShare> chromosomeInfoMap = new ConcurrentHashMap<String, ChromosomeInformationShare>();	
	
	public boolean loadGenome(String refList, String dbsnpList) {
		try {
			loadChromosomeList(refList);
		} catch (Exception e) {
			return false;
		}
		return true;
	}
	
	/**
	 * 循环调用loadChr方法，分别映射染色体文件，获得染色体长度
	 * @param chrList	包含所有染色体文件路径的list文件
	 * @throws IOException
	 */
	public void loadChromosomeList(String chrList) throws IOException{
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
			if(!addChromosome(chrs[0])) {
				StringBuilder errorDescription = new StringBuilder();
				errorDescription.append("map Chromosome ");
				errorDescription.append(chrs[1]);
				errorDescription.append(" Failed.");
				System.err.println(errorDescription.toString());
			}
			if (chromosomeInfoMap.containsKey(chrs[0])) {
				// map chr and get length
				chromosomeInfoMap.get(chrs[0]).loadChromosome(chrs[1]);
				chromosomeInfoMap.get(chrs[0]).setLength(Integer.parseInt(chrs[2]));
				chromosomeInfoMap.get(chrs[0]).setChromosomeName(chrs[0]);
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
	public void loadChromosomeList() throws IOException{
		BufferedReader br = new BufferedReader(new FileReader(new File("refList")));
		
		String line = new String();
		while((line = br.readLine()) != null) {
			String[] chrs = line.split("\t");
			System.err.println(line);
			// insert chr
			if(!addChromosome(chrs[0])) {
				StringBuilder errorDescription = new StringBuilder();
				errorDescription.append("map Chromosome ");
				errorDescription.append(chrs[1]);
				errorDescription.append(" Failed.");
				System.err.println(errorDescription.toString());
			}
			if (chromosomeInfoMap.containsKey(chrs[0])) {
				// map chr and get length
				chromosomeInfoMap.get(chrs[0]).loadChromosome(chrs[0] + "ref");
				chromosomeInfoMap.get(chrs[0]).setLength(Integer.parseInt(chrs[2]));
				chromosomeInfoMap.get(chrs[0]).setChromosomeName(chrs[0]);
			}
		}
		br.close();
	}
	
	/**
	 * 循环调用loadChr方法，分别映射染色体文件，获得染色体长度
	 * @param chrList	包含所有染色体文件路径的list文件
	 * @throws IOException
	 */
	public void loadBasicInformation(String chrList) throws IOException{
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
			if(!addChromosome(chrs[0])) {
				StringBuilder errorDescription = new StringBuilder();
				errorDescription.append("map Chromosome ");
				errorDescription.append(chrs[1]);
				errorDescription.append(" Failed.");
				System.err.println(errorDescription.toString());
			}
			if (chromosomeInfoMap.containsKey(chrs[0])) {
				// map chr and get length
				chromosomeInfoMap.get(chrs[0]).setLength(Integer.parseInt(chrs[2]));
				chromosomeInfoMap.get(chrs[0]).setChromosomeName(chrs[0]);
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
	
	public void loadDbSNPList(String dbsnpListPath) throws IOException{
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
				chromosomeInfoMap.get(chromosome).loadDbSNP(dbsnpPath, indexPath, size);
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
	
	public void loadDbSNPList() throws IOException{
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
				chromosomeInfoMap.get(chromosome).loadDbSNP(dbsnpPath, indexPath, size);
			}
		}
		br.close();
	}
	
	/**
	 * 获取染色体信息Map
	 * @return
	 */
	public Map<String, ChromosomeInformationShare> getChromosomeInfoMap() {
		return chromosomeInfoMap;
	}

	/**
	 * 根据名称获取染色体信息
	 * @param chrName
	 * @return
	 */
	public ChromosomeInformationShare getChromosomeInfo(String chrName) {
		if(chromosomeInfoMap.get(ChromosomeUtils.formatChrName(chrName)) == null)
			throw new RuntimeException("chr name not in GaeaIndex of ref:" + chrName);
		return chromosomeInfoMap.get(ChromosomeUtils.formatChrName(chrName));
	}

	/**
	 * 添加染色体信息
	 * @param chrName 染色体名称
	 * @return
	 */
	public boolean addChromosome(String chrName){		
		ChromosomeInformationShare newChr = new ChromosomeInformationShare();
		chromosomeInfoMap.put(chrName, newChr);
		if(chromosomeInfoMap.get(chrName) != null) {
			return true;
		} else {
			return false;
		}
	}
}
