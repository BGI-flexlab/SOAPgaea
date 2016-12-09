/**
 * Copyright (c) 2011, BGI and/or its affiliates. All rights reserved.
 * BGI PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package org.bgi.flexlab.gaea.data.structure.reference;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.bgi.flexlab.gaea.data.structure.memoryshare.BioMemoryShare;
import org.bgi.flexlab.gaea.util.ChromosomeUtils;
import org.bgi.flexlab.gaea.util.SystemConfiguration;

/**
 * 染色体信息共享内存
 * 
 * @author ZhangYong
 *
 */
public class ChromosomeInformationShare extends BioMemoryShare {
	
	public ChromosomeInformationShare(){
		super(Byte.SIZE / 4);
	}

	/**
	 * get base from reference position
	 * 
	 * @param position
	 * @return base
	 */
	public byte getBinaryBase(int pos) {
		byte curr = getBytes(pos,pos)[0];
		
		if((pos & 0x1) == 0)
			return (byte)(curr & 0x0f);
		return (byte)((curr >> 4) & 0x0f);
	}

	/**
	 * 获取碱基
	 */
	public char getBase(int pos) {
		return SystemConfiguration.getFastaAbb(getBinaryBase(pos));
	}
	
	public boolean isSNP(int pos){
		byte posByte = getBinaryBase(pos);
		
		if(((posByte >> 3) & 0x1) == 0)
			return false;
		return true;
	}
	
	public BaseAndSNPInformation getInformation(int start,int end){
		byte[] bytes = getBytes(start,end);
		BaseAndSNPInformation info = new BaseAndSNPInformation(bytes,start);
		return info;
	}

	/**
	 * 获取染色体序列
	 * 
	 * @param start
	 *            从0开始
	 * @param end
	 * @return String 序列
	 */
	public String getBaseSequence(int start, int end) {
		StringBuffer seq = new StringBuffer();
		byte[] bases = getBytes(start, end);

		if ((start & 0x1) == 0) {
			seq.append(SystemConfiguration.getFastaAbb(bases[0] & 0x0f));
			seq.append(SystemConfiguration.getFastaAbb((bases[0] >> 4) & 0x0f));
		} else {
			seq.append(SystemConfiguration.getFastaAbb((bases[0] >> 4) & 0x0f));
		}
		// 取一个位点
		if (start == end) {
			return seq.toString();
		}
		for (int i = 1; i < bases.length - 1; i++) {
			seq.append(SystemConfiguration.getFastaAbb(bases[i] & 0x0f));
			seq.append(SystemConfiguration.getFastaAbb((bases[i] >> 4) & 0x0f));
		}
		if ((end & 0x1) == 0) {
			seq.append(SystemConfiguration
					.getFastaAbb(bases[bases.length - 1] & 0x0f));
		} else {
			seq.append(SystemConfiguration
					.getFastaAbb(bases[bases.length - 1] & 0x0f));
			seq.append(SystemConfiguration
					.getFastaAbb((bases[bases.length - 1] >> 4) & 0x0f));
		}
		return seq.toString();
	}
	
	public byte[] getBaseBytes(int start,int end){
		return getBaseSequence(start,end).getBytes();
	}

	/**
	 * 转换byte数组为字符串
	 * 
	 * @param b
	 *            输入byte数组
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
	 */
	public static double byteToDouble(byte[] b, int i) {
		long l = 0;
		for (int j = 0; j < 8; j++) {
			l |= (((long) (b[i + j] & 0xff)) << (8 * j));
		}
		return Double.longBitsToDouble(l);
	}
	
	public static void main(String[] args) throws IOException{
		String input = args[0];
		String chrName = ChromosomeUtils.formatChrName(args[1]);
		
		HashMap<String,String> map = new HashMap<String,String>();
		
		BufferedReader br = new BufferedReader(new FileReader(input));
		
		String line ;
		while((line = br.readLine()) != null){
			String[] str = line.split("\t");
			map.put(str[0], line);
		}
		
		br.close();
		
		if(!map.containsKey(chrName))
			throw new RuntimeException(chrName+" is not exist!");
		
		String[] str = map.get(chrName).split("\t");
		ChromosomeInformationShare chr = new ChromosomeInformationShare();
		chr.setChromosomeName(chrName);
		chr.setLength(Integer.parseInt(str[2]));
		
		try {
			chr.loadInformation(str[1]);
		} catch (IOException e) {
			throw new RuntimeException(e.toString());
		}
		
		for(int i = 0 ; i < chr.getLength() ; i++){
			if(chr.isSNP(i)){
				System.out.println((i+1)+"\t"+chr.getBase(i));
			}
		}
		
		map.clear();
	}
}
