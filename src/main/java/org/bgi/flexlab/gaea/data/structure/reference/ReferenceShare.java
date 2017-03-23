/**
 * Copyright (c) 2011, BGI and/or its affiliates. All rights reserved.
 * BGI PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package org.bgi.flexlab.gaea.data.structure.reference;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.LineReader;
import org.bgi.flexlab.gaea.data.structure.memoryshare.WholeGenomeShare;
import org.bgi.flexlab.gaea.util.ChromosomeUtils;

/**
 * Genome内存共享
 * 
 * @author ZhangZhi && ChuanJun && ZhangYong
 *
 */
public class ReferenceShare extends WholeGenomeShare {
	private static final String CACHE_NAME = "refList";

	/**
	 * 染色体信息Map
	 */
	private Map<String, ChromosomeInformationShare> chromosomeInfoMap = new ConcurrentHashMap<String, ChromosomeInformationShare>();

	public static boolean distributeCache(String chrList, Job job) {
		try {
			return distributeCache(chrList, job, CACHE_NAME);
		} catch (IOException e) {
			throw new RuntimeException(e.toString());
		} catch (URISyntaxException e) {
			throw new RuntimeException(e.toString());
		}
	}

	public void loadChromosomeList(String chrList) {
		try {
			loadChromosomeList(new Path(chrList));
		} catch (IllegalArgumentException | IOException e) {
			throw new RuntimeException(e.toString());
		}
	}

	public void loadChromosomeList() {
		loadChromosomeList(CACHE_NAME);
	}

	/**
	 * 循环调用loadChr方法，分别映射染色体文件，获得染色体长度
	 * 
	 * @param chrList
	 *            包含所有染色体文件路径的list文件
	 * @throws IOException
	 */
	public void loadBasicInformation(String chrList) throws IOException {
		Configuration conf = new Configuration();
		Path refPath = new Path(chrList);
		FileSystem fs = refPath.getFileSystem(conf);
		FSDataInputStream refin = fs.open(refPath);
		LineReader in = new LineReader(refin);
		Text line = new Text();

		String chrFile = "";
		String[] chrs = new String[3];
		while ((in.readLine(line)) != 0) {
			chrFile = line.toString();
			chrs = chrFile.split("\t");

			// insert chr
			if (!addChromosome(chrs[0])) {
				in.close();
				throw new RuntimeException("map Chromosome " + chrs[1] + " Failed.");
			}
			if (chromosomeInfoMap.containsKey(chrs[0])) {
				// map chr and get length
				chromosomeInfoMap.get(chrs[0]).setLength(Integer.parseInt(chrs[2]));
				chromosomeInfoMap.get(chrs[0]).setChromosomeName(chrs[0]);
			}
		}
		in.close();
	}

	/**
	 * 获取染色体信息Map
	 * 
	 * @return
	 */
	public Map<String, ChromosomeInformationShare> getChromosomeInfoMap() {
		return chromosomeInfoMap;
	}

	/**
	 * 根据名称获取染色体信息
	 * 
	 * @param chrName
	 * @return
	 */
	public ChromosomeInformationShare getChromosomeInfo(String chrName) {
		if (chromosomeInfoMap.get(ChromosomeUtils.formatChrName(chrName)) == null)
			throw new RuntimeException("chr name not in GaeaIndex of ref:" + chrName);
		//FIXME::temporary solution for chromosome name problem.
		ChromosomeInformationShare chrInfo = chromosomeInfoMap.get(ChromosomeUtils.formatChrName(chrName));
		chrInfo.setChromosomeName(chrName);
		return chrInfo;
	}

	/**
	 * 添加染色体信息
	 * 
	 * @param chrName
	 *            染色体名称
	 * @return
	 */
	public boolean addChromosome(String chrName) {
		ChromosomeInformationShare newChr = new ChromosomeInformationShare();
		chromosomeInfoMap.put(chrName, newChr);
		if (chromosomeInfoMap.get(chrName) != null) {
			return true;
		} else {
			return false;
		}
	}

	public void setChromosome(String path, String chrName, int length) {
		if (chromosomeInfoMap.containsKey(chrName)) {
			// map chr and get length
			chromosomeInfoMap.get(chrName).loadChromosome(path);
			chromosomeInfoMap.get(chrName).setLength(length);
			chromosomeInfoMap.get(chrName).setChromosomeName(chrName);
		}
	}

	@Override
	public void clean() {
		for (String key : chromosomeInfoMap.keySet()) {
			ChromosomeInformationShare share = chromosomeInfoMap.get(key);
			try {
				share.clean();
			} catch (Exception e) {
				throw new RuntimeException(e.toString());
			}
		}
	}
}
