/**
 * Copyright (c) 2011, BGI and/or its affiliates. All rights reserved.
 * BGI PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package org.bgi.flexlab.gaea.util;

import java.util.MissingResourceException;
import java.util.ResourceBundle;


public class SystemConfiguration {
	/**
	 * BAM flags
	 */
	/*! @abstract the read is paired in sequencing, no matter whether it is mapped in a pair */
	public static final int BAM_FPAIRED = 1;
	/*! @abstract the read is mapped in a proper pair */
	public static final int BAM_FPROPER_PAIR = 2;
	/*! @abstract the read itself is unmapped; conflictive with BAM_FPROPER_PAIR */
	public static final int BAM_FUNMAP = 4;
	/*! @abstract the mate is unmapped */
	public static final int BAM_FMUNMAP =8;
	/*! @abstract the read is mapped to the reverse strand */
	public static final int BAM_FREVERSE = 16;
	/*! @abstract the mate is mapped to the reverse strand */
	public static final int BAM_FMREVERSE = 32;
	/*! @abstract this is read1 */
	public static final int BAM_FREAD1 = 64;
	/*! @abstract this is read2 */
	public static final int BAM_FREAD2 = 128;
	/*! @abstract not primary alignment */
	public static final int BAM_FSECONDARY = 256;
	/*! @abstract QC failure */
	public static final int BAM_FQCFAIL = 512;
	/*! @abstract optical or PCR duplicate */
	public static final int BAM_FDUP = 1024;
	
	/*
	  CIGAR operations.
	 */
	/*! @abstract CIGAR: M = match or mismatch*/
	public static final int BAM_CMATCH = 0;
	/*! @abstract CIGAR: I = insertion to the reference */
	public static final int BAM_CINS = 1;
	/*! @abstract CIGAR: D = deletion from the reference */
	public static final int BAM_CDEL = 2;
	/*! @abstract CIGAR: N = skip on the reference (e.g. spliced alignment) */
	public static final int BAM_CREF_SKIP = 3;
	/*! @abstract CIGAR: S = clip on the read with clipped sequence present in qseq */
	public static final int BAM_CSOFT_CLIP = 4;
	/*! @abstract CIGAR: H = clip on the read with clipped sequence trimmed off */
	public static final int BAM_CHARD_CLIP = 5;
	/*! @abstract CIGAR: P = padding */
	public static final int BAM_CPAD = 6;
	/*! @abstract CIGAR: equals = match */
	public static final int BAM_CEQUAL = 7;
	/*! @abstract CIGAR: X = mismatch */
	public static final int BAM_CDIFF = 8;
	public static final int BAM_CBACK = 9;
	
	public static final int indelCallingWindowSize = 1000;
	
	/**
	 * 存储碱基信息（4bit）的个数
	 */
	private static final int CAPACITY;

	/**
	 * 二倍体基因型FASTA格式编码(A,C,G,T)
	 */
	private static final char[] FASTA_ABBR;
	
	/**
	 * VCF中基因型FASTA格式编码(A,C,G,T)
	 */
	private static final String[] FASTA_VABBR;

	/**
	 *  所使用的ResourceBundle
	 */
	private static ResourceBundle bundle;
	
	/**
	 *  静态初始化块，用于加载属性文件
	 */
	static {
		CAPACITY = Byte.SIZE / 4;
		FASTA_ABBR = new char[] {'A','M','W','R','M','C','Y','S','W','Y','T','K','R','S','K','G','N'};
		FASTA_VABBR = new String[] {"A", "A,C", "A,T", "A,G", "A,C", "C", "C,T", "C,G", "A,T", "C,T", "T", "G,T", "A,G", "C,G", "G,T", "G", "N"};
		StringBuilder pathBuilder = new StringBuilder();
		pathBuilder.append("properties");
		pathBuilder.append(System.getProperty("file.separator"));
		pathBuilder.append("GaeaSNP");
		try {
			bundle = ResourceBundle.getBundle(pathBuilder.toString());
		} catch (MissingResourceException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 获取长整形变量的存储空间大小
	 * @param
	 * @return int
	 */
	public static int getCapacity() {
		return CAPACITY;
	}

	/**
	 * 获取FASTA格式的字母表
	 * @param
	 * @return char
	 */
	public static char getFastaAbbr(int index) {
		return FASTA_ABBR[index];
	}
	
	/**
	 * VCF获取FASTA格式的字母表
	 * @param
	 * @return String
	 */
	public static String getFastaVabbr(int index) {
		return FASTA_VABBR[index];
	}
	
	/**
	 *  从属性文件中取得属性值
	 * @param key
	 * @return
	 */
	private static String getValue(String key) {
		return bundle.getString(key);
	}
	
	/**
	 *  获取CNS格式输出文件的文件夹名称
	 * @return
	 */
	public static String getOutputCNSDir() {
		return getValue("GaeaSNP.outputCNSDir");
	}
	
	/**
	 *  获取CNS格式输出文件的文件夹名称
	 * @return
	 */
	public static String getOutputVCFDir() {
		return getValue("GaeaSNP.outputVCFDir");
	}
	
	/**
	 *  获取输出路径中的临时文件夹名称
	 * @return
	 */
	public static String getOutputTmpDir() {
		return getValue("GaeaSNP.outputTMPDir");
	}
	
	/**
	 *  获取输出路径中的质量校正矩阵文件夹名称
	 * @return
	 */
	public static String getOutputRecalMatrixDir() {
		return getValue("GaeaSNP.outputRecalMatrixDir");
	}
	
	/**
	 *  获取集群节点个数
	 * @return
	 */
	public static int getNodeCount() {
		return Integer.parseInt(getValue("GaeaSNP.nodeCount"));
	}
	
	/**
	 *  获取每个节点任务槽个数
	 * @return
	 */
	public static int getTaskSlotCount() {
		return Integer.parseInt(getValue("GaeaSNP.taskSlotCount"));
	}
	
	/**
	 *  获取窗口大小
	 * @return
	 */
	public static int getWindowSize() {
		return Integer.parseInt(getValue("GaeaSNP.windowSize"));
	}
	
	public static final int  VC_NO_GENO= 2;
	public static final int VC_BCFOUT=  4;
	public static final int VC_CALL   = 8;
	public static final int VC_VARONLY =16;
	public static final int VC_VCFIN  = 32;
	public static final int VC_UNCOMP  =64;
	public static final int VC_KEEPALT =256;
	public static final int VC_ACGT_ONLY =512;
	public static final int VC_QCALL   =1024;
	public static final int VC_CALL_GT =2048;
	public static final int VC_ADJLD  = 4096;
	public static final int VC_NO_INDEL =8192;
	public static final int VC_ANNO_MAX =16384;
	public static final int VC_FIX_PL  = 32768;
	public static final int VC_EM      = 0x10000;
	public static final int VC_PAIRCALL =0x20000;
	public static final int VC_QCNT    = 0x40000;
	
	public static int B2B_INDEL_NULL= 10000;
	
	public static int RAND_MAX=0x7fff;
	
	public static int DEF_MAPQ =20;
	
	public static int CAP_DIST=25;
	
	public static int BAM_CIGAR_SHIFT=4;
	
	public static int BAM_CIGAR_MASK=(1 << BAM_CIGAR_SHIFT) - 1;
	
	public static int MINUS_CONST =0x10000000;
	
	public static int INDEL_WINDOW_SIZE= 50;
	
	public static double M_LN10 = 2.30258509299404568402;
	
	public static double M_LN2 = 0.693147180559945309417;
	
	public static float CALL_DEFTHETA=0.83f;
	
	public static int bam_nt16_nt4_table[] = { 4, 0, 1, 4, 2, 4, 4, 4, 3, 4, 4, 4, 4, 4, 4, 4 };
	
	public static int bam_nt16_table[] = {
		15,15,15,15, 15,15,15,15, 15,15,15,15, 15,15,15,15,
		15,15,15,15, 15,15,15,15, 15,15,15,15, 15,15,15,15,
		15,15,15,15, 15,15,15,15, 15,15,15,15, 15,15,15,15,
		 1, 2, 4, 8, 15,15,15,15, 15,15,15,15, 15, 0 /*=*/,15,15,
		15, 1,14, 2, 13,15,15, 4, 11,15,15,12, 15, 3,15,15,
		15,15, 5, 6,  8,15, 7, 9, 15,10,15,15, 15,15,15,15,
		15, 1,14, 2, 13,15,15, 4, 11,15,15,12, 15, 3,15,15,
		15,15, 5, 6,  8,15, 7, 9, 15,10,15,15, 15,15,15,15,
		15,15,15,15, 15,15,15,15, 15,15,15,15, 15,15,15,15,
		15,15,15,15, 15,15,15,15, 15,15,15,15, 15,15,15,15,
		15,15,15,15, 15,15,15,15, 15,15,15,15, 15,15,15,15,
		15,15,15,15, 15,15,15,15, 15,15,15,15, 15,15,15,15,
		15,15,15,15, 15,15,15,15, 15,15,15,15, 15,15,15,15,
		15,15,15,15, 15,15,15,15, 15,15,15,15, 15,15,15,15,
		15,15,15,15, 15,15,15,15, 15,15,15,15, 15,15,15,15,
		15,15,15,15, 15,15,15,15, 15,15,15,15, 15,15,15,15
	};
	
	/**
	 * 单倍体基因型FASTA格式编码(A,C,G,T)
	 */
	public static final char[] FASTA_ABB;
	
	/**
	 *  静态初始化块，用于加载属性文件
	 */
	static {
		FASTA_ABB = new char[] {'A', 'C', 'T', 'G'};
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
	
private static ResourceBundle bundleGaeaAlignment;
	
	static{
		StringBuilder pathBuilder = new StringBuilder();
		pathBuilder.append("properties");
		pathBuilder.append(System.getProperty("file.separator"));
		pathBuilder.append("GaeaAlignment");
		bundleGaeaAlignment = ResourceBundle.getBundle(pathBuilder.toString());
	}
	
	private static String getGaeaAlignmentValue(String key){
		return bundleGaeaAlignment.getString(key);
	}
	
	public static String getReference(String refVersion){
		String path = null;
		if(refVersion.equals("hg18")){
			path = getGaeaAlignmentValue("GaeaAlignment.refHg18Path");
		}else if(refVersion.equals("hg19")){
			path = getGaeaAlignmentValue("GaeaAlignment.refHg19Path");
		}else if(refVersion.equals("ncbi37")){
			path = getGaeaAlignmentValue("GaeaAlignment.refNcbi37Path");
		}else{
			throw new IllegalArgumentException("Unrecognized argument for reference version: " + 
					refVersion + "\n\tit must be one of {hg18, hg19, ncbi37}");
		}
		return path;
	}
	
	public static String getAlnOutDir(){
		return getGaeaAlignmentValue("GaeaAlignment.alnoutDir");
	}
	
	public static String getAlnSeqDir(){
		return getGaeaAlignmentValue("GaeaAlignment.alnseqDir");
	}
	
	public static String getSamOutDir(){
		return getGaeaAlignmentValue("GaeaAlignment.samoutDir");
	}
	
	public static String getTempDir(){
		return getGaeaAlignmentValue("GaeaAlignment.tempDir");
	}
	
	public static String getRefsName(){
		return getGaeaAlignmentValue("GaeaAlignment.refsDir");
	}
	
	public static String getChrsName(){
		return getGaeaAlignmentValue("GaeaAlignment.chrsDir");
	}
	
	public static String getIndexName(){
		return getGaeaAlignmentValue("GaeaAlignment.indexDir");
	}
	
	public static String getChrListName(){
		return getGaeaAlignmentValue("GaeaAlignment.chrlistName");
	}
	
}