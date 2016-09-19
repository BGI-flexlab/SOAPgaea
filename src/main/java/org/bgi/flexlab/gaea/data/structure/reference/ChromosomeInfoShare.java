/**
 * Copyright (c) 2011, BGI and/or its affiliates. All rights reserved.
 * BGI PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package org.bgi.flexlab.gaea.data.structure.reference;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

/**
 * 染色体信息共享内存
 * @author ZhangYong
 *
 */
public class ChromosomeInfoShare {
	
	private static int CAPACITY = Byte.SIZE / 4;
	
	public static final char[] FASTA_ABB = new char[] {'A', 'C', 'T', 'G'};
	
	/**
	 * 染色体名称
	 */
	private String chrName;
	
	/**
	 * 染色体对应参考基因组长度
	 */
	private int length;

	/**
	 * 二进制格式的序列。 每4个bit表示一个碱基的信息: 第一位表示dbSNPstatus 第二位表示N 最后两位表示碱基类型，其中A: 00, C:
	 * 01, T: 10, G:11 每个long类型变量可以存储16个碱基的信息
	 */
	private MappedByteBuffer[] refSeq; // reference seq map
	
	/**
	 * dbSNP 信息
	 */
	private ChromosomeDbSNPShare dbsnpInfo;

	/**
	 * @return the chrName
	 */
	public String getChrName() {
		return chrName;
	}

	/**
	 * @param chrName the chrName to set
	 */
	public void setChrName(String chrName) {
		this.chrName = chrName;
	}

	/**
	 * 设置染色体对应参考基因组长度
	 * 
	 * @param chrLen 染色体长度
	 */
	public void setLength(int chrLen) {
		length = chrLen;
	}

	/**
	 * 获取染色体对应参考基因组长度
	 * 
	 * @param
	 * @return int
	 */
	public int getLength() {
		return length;
	}

	/**
	 * 映射一条染色体文件到内存
	 * @param chr 染色体文件名
	 * @throws IOException
	 */
	public void loadChr(String chr) throws IOException {
		RandomAccessFile raf = new RandomAccessFile(chr, "r");
		FileChannel fc = raf.getChannel();
		int blocks = (int) ((fc.size() / Integer.MAX_VALUE) + 1);
		MappedByteBuffer[] allMbb = new MappedByteBuffer[blocks];
		int start = 0;
		long remain = 0;
		int size = 0;
		for (int i = 0; i < blocks; i++) {
			start = Integer.MAX_VALUE * i;
			remain = (long) (fc.size() - start);
			size = (int) ((remain > Integer.MAX_VALUE) ? Integer.MAX_VALUE : remain);
			MappedByteBuffer mapedBB = fc.map(MapMode.READ_ONLY, start, size);
			allMbb[i] = mapedBB;
		}
		raf.close();
		refSeq = allMbb;
	}
	
	/**
	 * 映射dbSNP信息到内存
	 * 有dbsnp信息才初始化dbsnpInfo
	 * @throws IOException
	 * @param dbsnpPath dbSNP path
	 * @return 0
	 */
	public void loaddbSNP(String dbsnpPath, String indexPath, int size) throws IOException {
		dbsnpInfo = new ChromosomeDbSNPShare(dbsnpPath, indexPath, size);
	}
	
	/**
	 * 获取dbsnp信息
	 * 
	 * @param
	 * @return ChromosomeDbSNPShare
	 */
	public ChromosomeDbSNPShare getDbsnpInfo() {
		return dbsnpInfo;
	}

	/**
	 * 按参考基因组序列的位置获取一个碱基编码
	 * 
	 * @param pos position
	 * @return base
	 */
	public byte getBinaryBase(int pos) {
		int posi = pos / CAPACITY;
		byte base;

		refSeq[0].position(posi);
		base = refSeq[0].get();
		refSeq[0].position(0);

		if (pos % 2 == 0) {
			base &= 0x0f;
		}
		if (pos % 2 == 1) {
			base >>= 4; 
			base &= 0x0f;
		}
		return base;
	}
	
	/**
	 * 获取碱基
	 * @param pos
	 * @return
	 */
	public char getBase(int pos) {
		if(pos >= length) return 0;
		
		int posi = pos / CAPACITY;
		byte base;

		refSeq[0].position(posi);
		base = refSeq[0].get();
		refSeq[0].position(0);

		if (pos % 2 == 0) {
			base &= 0x0f;
		}
		if (pos % 2 == 1) {
			base >>= 4; 
			base &= 0x0f;
		}
		return getFastaAbb(base);
	}

	/**获取染色体序列
	 * @param start 从0开始
	 * @param end
	 * @return String 序列
	 */
	public String getBaseSeq(int start, int end) {
		if(start >= length) {
			return "";
		}
		
		StringBuffer seq = new StringBuffer();
		byte[] bases;
		
		int posi = start / CAPACITY;
		int pose;
		if(end >= length) {
			System.err.println("seq end has reach the end of chromesome");
			pose = (length - 1) / CAPACITY;
		} else {
			pose = end / CAPACITY;
		}
		bases = new byte[pose - posi + 1];
		refSeq[0].position(posi);
		refSeq[0].get(bases, 0, pose - posi + 1);
		refSeq[0].position(0);
		
		if (start % 2 == 0) {
			seq.append(getFastaAbb(bases[0] & 0x0f));
			seq.append(getFastaAbb((bases[0] >> 4)& 0x0f));
		}
		if (start % 2 == 1) {
			seq.append(getFastaAbb((bases[0] >> 4)& 0x0f));
		}
		//取一个位点
		if(start == end) {
			return seq.toString();
		}
		for(int i = 1; i < bases.length - 1; i++) {
			seq.append(getFastaAbb(bases[i] & 0x0f));
			seq.append(getFastaAbb((bases[i] >> 4)& 0x0f));
		}
		if (end % 2 == 0) {
			seq.append(getFastaAbb(bases[bases.length - 1] & 0x0f));
		}
		if (end % 2 == 1) {
			seq.append(getFastaAbb(bases[bases.length - 1] & 0x0f));
			seq.append(getFastaAbb((bases[bases.length - 1] >> 4)& 0x0f));
		}
		return seq.toString();
	}
	
	/**
	 * 获取单倍字母表
	 */
	public static char getFastaAbb(int code) {
		int index = (code & 0x07);
		if((index & 0x04) != 0) {
			return 'N';
		}
		else
			return FASTA_ABB[index];
	}

	/**
	 * 转换byte数组为字符串
	 * @param b 输入byte数组
	 * @return 返回相应的字符串
	 */
	public static String bytes2String(byte[] b) {
		StringBuilder sb = new StringBuilder(b.length);
		for (byte each : b) {
			sb.append((char) each);
		}
		return sb.toString();
	}

	/**
	 * byte转换为double类型
	 * @param b
	 * @param i
	 * @return
	 */
	public static double byteToDouble(byte[] b, int i) {
		long l = 0;
		for (int j = 0; j < 8; j++) {
			l |= (((long) (b[i + j] & 0xff)) << (8 * j));
		}
		return Double.longBitsToDouble(l);
	}
}

