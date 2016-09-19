/**
 * Copyright (c) 2011, BGI and/or its affiliates. All rights reserved.
 * BGI PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package org.bgi.flexlab.gaea.data.structure.reference;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 建立参考基因组和dbSNP数据文件索引
 * @author ZhangYong, ZhangZhi
 *
 */
public class BuildIndex {
	/**
	 * 染色体信息Map
	 */
	private static Map<String, ChrInfo> chromosomeInfoMap = new ConcurrentHashMap<String, ChrInfo>();
	
	
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
	 * 读取参考基因组的FASTA格式序列文件
	 * @param refSeqPath 参考基因组路径
	 * @throws IOException
	 */
	public static void readRefSeq(String refSeqPath) throws IOException {
		if (null != refSeqPath && !refSeqPath.equals("")) {
			File in = new File(refSeqPath);
			BufferedReader reader = new BufferedReader(new FileReader(in));

			String line = null;
			StringBuilder refSeq = new StringBuilder();
			String curChrName = "";
			// 读取参考基因组序列
			while ((line = reader.readLine()) != null) {
				// '>'字符是一个染色体序列信息的起始标志
				if (line.length() != 0 && '>' == line.charAt(0)) {
					// 处理之前的一个染色体
					if (chromosomeInfoMap.containsKey(curChrName)) {
						// 二进制化染色体碱基序列
						chromosomeInfoMap.get(curChrName).setBinarySeq(refSeq.toString());
						System.out.println("> Finished loading chromosome: " + curChrName);
					}

					// 获取染色体名称
					int pos;
					for (pos = 1; pos != line.length() && '\t' != line.charAt(pos) && '\n' != line.charAt(pos) && ' ' != line.charAt(pos) && '\r' != line.charAt(pos) && '\f' != line.charAt(pos) ; pos++) {
					}
					String newChrName = getChrnameFormat(line.substring(1, pos));

					// 判断添加染色体信息是否成功
					if (!addChr(newChrName)) {
						StringBuilder errorDescription = new StringBuilder();
						errorDescription.append("> Insert Chromosome ");
						errorDescription.append(newChrName);
						errorDescription.append(" Failed.");
						System.err.println(errorDescription.toString());
					}
					curChrName = newChrName;
					refSeq.setLength(0);
				} else {
					refSeq.append(line);
				}
			}
			// 处理最后一个染色体的信息
			if (refSeq.length() != 0 && chromosomeInfoMap.containsKey(curChrName)) {
				// 二进制化染色体碱基序列
				chromosomeInfoMap.get(curChrName).setBinarySeq(refSeq.toString());
			}
			reader.close();
		} else {
			System.err.println("> Empty path of refSeq file.");
		}
	}

	/**
	 * 设置SNP基本信息位数组
	 * @param info 一行dbSNP信息
	 * @return snpBasicInfo 设置好的基本信息位数组
	 */
	private static byte getSnpBasicInfo(String[] info) {
		byte snpBasicInfo = 0;

		// IsValidated信息
		if (info[3].trim().equals("1")) {
			snpBasicInfo |= 1;
		}

		// IsHapMap信息
		if (info[2].trim().equals("1")) {
			snpBasicInfo |= 2;
		}

		// FreqG
		if (!info[8].trim().equals("0")) {
			snpBasicInfo |= 4;
		}
		// FreqT
		if (!info[7].trim().equals("0")) {
			snpBasicInfo |= 8;
		}
		// FreqC
		if (!info[6].trim().equals("0")) {
			snpBasicInfo |= 16;
		}
		// FreqA
		if (!info[5].trim().equals("0")) {
			snpBasicInfo |= 32;
		}

		return snpBasicInfo;
	}

	/**
	 * 读取dbSNP数据文件，插入到对应的参考基因组数据中；编码并二进制输出dbSNP数据文件
	 * @param dbSNPPath dbSNP文件列表路径
	 * @param outPath 编码压缩后的dbSNP输出目录
	 * @param outIndexPath dbSNP索引文件输出
	 * @return dbsnpNum 处理的dbSNP条数
	 * @throws IOException
	 */
	public static int readDbSnp(String dbSNPPath, String outPath, String outIndexPath) throws IOException {
		int dbsnpNum = 0;
		if(null != dbSNPPath && !dbSNPPath.equals("")) {
			DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(outPath)));
			DataOutputStream index = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(outIndexPath)));

			File in = new File(dbSNPPath);
			BufferedReader reader = new BufferedReader(new FileReader(in));

			int pos = -1;
			String currentName = null;
			String[] splitArray = null;
			String line = null;
			// 按行读取dbSNP数据文件
			while ((line = reader.readLine()) != null && line.length() != 0) {
				// dbSNP数据格式: Chr\tPos\thapmap?\tvalidated?\tis_indel?\tA\tC\tT\tG\trsID\n
				splitArray = line.split("\t");

				// 如果该位点为Indel，则不添加该SNP信息
				if (splitArray[4].trim().equals("1")) {
					continue;
				}

				// 等位基因频率数组
				float[] freq = {Float.parseFloat(splitArray[5].trim()), Float.parseFloat(splitArray[6].trim()), Float.parseFloat(splitArray[7].trim()), Float.parseFloat(splitArray[8].trim())} ;
				byte alleleCount = 0;
				byte zeroCount = 0;
				int nonZeroPos = 0; // 非零等位基因位置
				// 计算存在于dbSNP中的等位基因个数
				for (int base = 0; base < 4; base++) {
					if (freq[base] > 0) { // 等位基因频率大于0
						alleleCount += 1;
						if(alleleCount == 1)
							nonZeroPos = base;
					} else {
						zeroCount += 1;
					}
				}
				// 存在于dbSNP中的等位基因个数小于2，或等位基因频率都为0时，则不添加该SNP信息
				if (alleleCount < 2 || zeroCount == 4) {
					continue;
				}

				// 染色体名称
				currentName = splitArray[0].trim().toLowerCase();
				if(!currentName.contains("chr")) {
					currentName = "chr" + currentName;
				}

				// 位点坐标
				pos = Integer.parseInt(splitArray[1].trim());
				index.writeInt(pos - 1);
				dbsnpNum++;

				// 设置SNP基本信息
				out.writeByte(getSnpBasicInfo(splitArray));

				// 设置等位基因频率
				out.writeFloat(freq[nonZeroPos]);

				ChrInfo curChrInfo = chromosomeInfoMap.get(currentName);
				if (null != curChrInfo) {
					// 向染色体信息中添加dbSNP信息
					if(pos < curChrInfo.getLength()) {
						pos -= 1;
						curChrInfo.insertSnpInfo(pos);
					}
				} else {
					System.err.println("> Failed appending dbSNP information. No information related to chromosome name: " + currentName);
				}
			}
			reader.close();
			out.close();
			index.close();
		} else {
			System.err.println("> Empty path of dbSNP file.");
		}
		return dbsnpNum;
	}

	/**
	 * 根据名称获取染色体信息
	 * @param chrName
	 * @return chromosomeInfoMap中对应染色体的refIndex类
	 */
	public ChrInfo getChromosomeInfo(String chrName) {
		return chromosomeInfoMap.get(chrName);
	}

	/**
	 * 向chromosomeInfoMap添加染色体信息
	 * @param chrName 染色体名称
	 * @return 成功（true）或者失败（false）
	 */
	public static boolean addChr(String chrName){
		ChrInfo newChr = new ChrInfo();
		chromosomeInfoMap.put(chrName, newChr);
		return (chromosomeInfoMap.get(chrName) != null) ? true : false;
	}

	/**
	 * 创建文件夹
	 * @param path 文件夹路径
	 */
	private static void createDir(String path) {
		try {
			File myFolderPath = new File(path.toString());
			if (!myFolderPath.exists()) {
				myFolderPath.mkdir();
			}
		} catch (Exception e) {
			System.out.println("> Failed in creating directory when building index.");
			e.printStackTrace();
		}
	}

	/**
	 * 获取绝对路径
	 * @param filePath 文件名当前路径
	 * @return absPath 文件绝对路径
	 */
	private static String absPath(String filePath){
		File file = new File(filePath);
		String abs = file.getAbsolutePath();
		return abs;
	}

	/**
	 * 主方法
	 * @param args
	 * @throws Exception
	 */
	public static void main(String args[]) throws Exception {
		// 解析参数
		if (args.length < 2) {
			System.err.println("> java -jar BuildIndex.jar referencePath dbsnpListPath(chr+\t+sortedDbsnpFilePath) outputPath");
			System.exit(1);
		} else if(args.length < 3){
			System.err.println("> java -jar BuildIndex.jar referencePath dbsnpListPath(chr+\t+sortedDbsnpFilePath) outputPath");
			System.err.println("You better add your dbsnp file if you have it.");
		}
		
		boolean hasdbSNP = true;
		String referencePath = args[0];			// 参考基因组序列文件路径
		String dbsnpListPath = "";			// dbSNP文件列表路径
		String outputPath = "";	// 库文件输出路径
		if(args.length == 2) {
			outputPath = absPath(args[1]);
			hasdbSNP = false;
		} 
		if(args.length == 3) {
			dbsnpListPath = args[1];
			outputPath = absPath(args[2]);
		}
		createDir(outputPath); // 建立文件夹

		// 读取参考基因组
		System.out.println("> Start to load reference data.");
		readRefSeq(referencePath);
		System.out.println("> Finished loading reference data.");

		// 读取dbSNP数据文件，插入到对应的参考基因组数据中；编码并二进制输出dbSNP数据文件
		if(hasdbSNP) {
			System.out.println("> Start to build dbSNP data files and index files, and intert dbSNP information into corresponding reference data.");
			StringBuilder outputDbSNPListName = new StringBuilder(); // dbSNP索引文件列表路径
			outputDbSNPListName.append(outputPath);
			outputDbSNPListName.append("/dbsnp_bn.list");
			FileWriter dbsnpIndexWriter = new FileWriter(new File(outputDbSNPListName.toString()));	// 输出dbSNP索引文件
			BufferedReader dbsnpReader = new BufferedReader(new FileReader(new File(dbsnpListPath))); // 读取dbSNP数据文件
			String line = null;
			String[] lineSplit = null;
			while ((line = dbsnpReader.readLine()) != null){
				lineSplit = line.split("\t");
				System.out.println("> Start to load dbSNP: " + lineSplit[0]);
				StringBuilder outputDbsnpPath = new StringBuilder(); // dbSNP输出文件路径
				outputDbsnpPath.append(outputPath);
				outputDbsnpPath.append("/");
				outputDbsnpPath.append(lineSplit[0]);
				outputDbsnpPath.append(".dbsnp.bn");
				StringBuilder outputIndexFilePath = new StringBuilder(); // dbSNP索引文件路径
				outputIndexFilePath.append(outputPath);
				outputIndexFilePath.append("/");
				outputIndexFilePath.append(lineSplit[0]);
				outputIndexFilePath.append(".dbsnp.index");
	
				int dbsnpNum = 0;
				// 读取dbSNP数据文件，插入到对应的参考基因组数据中；编码并二进制输出dbSNP数据文件
				dbsnpNum = readDbSnp(lineSplit[1], outputDbsnpPath.toString(), outputIndexFilePath.toString());
				// 输出dbSNP索引文件
				dbsnpIndexWriter.write(lineSplit[0]);
				dbsnpIndexWriter.write("\t");
				dbsnpIndexWriter.write(outputDbsnpPath.toString());
				dbsnpIndexWriter.write("\t");
				dbsnpIndexWriter.write(outputIndexFilePath.toString());
				dbsnpIndexWriter.write("\t");
				dbsnpIndexWriter.write(String.valueOf(dbsnpNum));
				dbsnpIndexWriter.write("\n");
				System.out.println("> Finished loading dbSNP: " + lineSplit[0]);
			}
			dbsnpIndexWriter.close();
			dbsnpReader.close();
			System.out.println("> Finished building dbSNP data files and index files, and interting dbSNP information into corresponding reference data.");
		}
			
		// 输出编码后的二进制库文件和对应的索引文件
		System.out.println("> Start to build reference data files and index files.");
		StringBuilder outputRefListPath = new StringBuilder(); // 编码后的二进制库文件的索引文件路径
		outputRefListPath.append(outputPath);
		outputRefListPath.append("/ref_bn.list");
		FileWriter refList = new FileWriter(new File(outputRefListPath.toString()));
		Iterator<Entry<String, ChrInfo>> iter = chromosomeInfoMap.entrySet().iterator();
		while (iter.hasNext()){
			Entry<String, ChrInfo> entry = iter.next();
			String chrName = entry.getKey();
			ChrInfo curChrInfo = entry.getValue();
			StringBuilder outputFileName = new StringBuilder();
			outputFileName.append(outputPath);
			outputFileName.append("/");
			outputFileName.append(chrName);
			outputFileName.append(".fa.bn");
			curChrInfo.outputChrInfo(outputFileName.toString()); // 输出编码后的二进制库文件
			
			// 输出编码后的二进制库文件的索引文件
			int length = curChrInfo.getLength();
			refList.write(chrName);
			refList.write("\t");
			refList.write(outputFileName.toString());
			refList.write("\t");
			refList.write(String.valueOf(length));
			refList.write("\n");
		}
		refList.close();
		System.out.println("> Finished building reference data files and index files.");
	}
}
