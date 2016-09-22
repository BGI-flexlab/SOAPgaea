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
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.bgi.flexlab.gaea.exception.NullFilePathException;

/**
 * create reference and dbSNP file index
 *
 * @author ZhangYong, ZhangZhi
 */
public class BuildIndex {
	/**
	 * chromosome information map
	 */
	private static Map<String, ChromosomeInformation> chromosomeInfoMap = new ConcurrentHashMap<String, ChromosomeInformation>();

	/**
	 * read fasta reference
	 * 
	 * @param refSeqPath
	 *            reference file path
	 * @throws IOException
	 */
	public static void readReferenceName(String refSeqPath) throws IOException {
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
						chromosomeInfoMap.get(curChrName).setBinarySequence(
								refSeq.toString());
						System.out.println("> Finished loading chromosome: "
								+ curChrName);
					}

					// 获取染色体名称
					int pos;
					for (pos = 1; pos != line.length()
							&& '\t' != line.charAt(pos)
							&& '\n' != line.charAt(pos)
							&& ' ' != line.charAt(pos)
							&& '\r' != line.charAt(pos)
							&& '\f' != line.charAt(pos); pos++) {
					}
					String newChrName = GenomeShare.getChromosomeNameFormat(line.substring(
							1, pos));

					// 判断添加染色体信息是否成功
					if (!addChromosome(newChrName)) {
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
			if (refSeq.length() != 0
					&& chromosomeInfoMap.containsKey(curChrName)) {
				// 二进制化染色体碱基序列
				chromosomeInfoMap.get(curChrName).setBinarySequence(
						refSeq.toString());
			}
			reader.close();
		} else {
			throw new NullFilePathException("input", "reference");
		}
	}

	/**
	 * 设置SNP基本信息位数组
	 * 
	 * @param info
	 *            一行dbSNP信息
	 * @return snpBasicInfo 设置好的基本信息位数组
	 */
	private static byte getSnpBasicInformation(String[] info) {
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
	 * 
	 * @param dbSNPPath
	 *            dbSNP文件列表路径
	 * @param outPath
	 *            编码压缩后的dbSNP输出目录
	 * @param outIndexPath
	 *            dbSNP索引文件输出
	 * @return dbsnpNum 处理的dbSNP条数
	 * @throws IOException
	 */
	public static int readDbSnp(String dbSNPPath, String outPath,
			String outIndexPath) throws IOException {
		int dbsnpNum = 0;
		if (null != dbSNPPath && !dbSNPPath.equals("")) {
			DataOutputStream out = new DataOutputStream(
					new BufferedOutputStream(new FileOutputStream(outPath)));
			DataOutputStream index = new DataOutputStream(
					new BufferedOutputStream(new FileOutputStream(outIndexPath)));

			File in = new File(dbSNPPath);
			BufferedReader reader = new BufferedReader(new FileReader(in));

			int pos = -1;
			String currentName = null;
			String[] splitArray = null;
			String line = null;
			// 按行读取dbSNP数据文件
			while ((line = reader.readLine()) != null && line.length() != 0) {
				// dbSNP数据格式:
				// Chr\tPos\thapmap?\tvalidated?\tis_indel?\tA\tC\tT\tG\trsID\n
				splitArray = line.split("\t");

				// 如果该位点为Indel，则不添加该SNP信息
				if (splitArray[4].trim().equals("1")) {
					continue;
				}

				// 等位基因频率数组
				float[] freq = { Float.parseFloat(splitArray[5].trim()),
						Float.parseFloat(splitArray[6].trim()),
						Float.parseFloat(splitArray[7].trim()),
						Float.parseFloat(splitArray[8].trim()) };
				byte alleleCount = 0;
				byte zeroCount = 0;
				int nonZeroPos = 0; // 非零等位基因位置
				// 计算存在于dbSNP中的等位基因个数
				for (int base = 0; base < 4; base++) {
					if (freq[base] > 0) { // 等位基因频率大于0
						alleleCount += 1;
						if (alleleCount == 1)
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
				if (!currentName.contains("chr")) {
					currentName = "chr" + currentName;
				}

				// 位点坐标
				pos = Integer.parseInt(splitArray[1].trim());
				index.writeInt(pos - 1);
				dbsnpNum++;

				// 设置SNP基本信息
				out.writeByte(getSnpBasicInformation(splitArray));

				// 设置等位基因频率
				out.writeFloat(freq[nonZeroPos]);

				ChromosomeInformation curChrInfo = chromosomeInfoMap.get(currentName);
				if (null != curChrInfo) {
					// 向染色体信息中添加dbSNP信息
					if (pos < curChrInfo.getLength()) {
						pos -= 1;
						curChrInfo.insertSnpInformation(pos);
					}
				} else {
					System.err
							.println("> Failed appending dbSNP information. No information related to chromosome name: "
									+ currentName);
				}
			}
			reader.close();
			out.close();
			index.close();
		} else {
			throw new NullFilePathException("input", "dbsnp");
		}
		return dbsnpNum;
	}

	/**
	 * 根据名称获取染色体信息
	 * 
	 * @param chrName
	 * @return chromosomeInfoMap中对应染色体的refIndex类
	 */
	public ChromosomeInformation getChromosomeInformation(String chrName) {
		return chromosomeInfoMap.get(chrName);
	}

	/**
	 * 向chromosomeInfoMap添加染色体信息
	 * 
	 * @param chrName
	 *            染色体名称
	 * @return 成功（true）或者失败（false）
	 */
	public static boolean addChromosome(String chrName) {
		ChromosomeInformation newChr = new ChromosomeInformation();
		chromosomeInfoMap.put(chrName, newChr);
		return (chromosomeInfoMap.get(chrName) != null) ? true : false;
	}
}
