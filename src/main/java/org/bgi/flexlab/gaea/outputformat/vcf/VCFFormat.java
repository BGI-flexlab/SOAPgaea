/**
 * Copyright (c) 2011, BGI and/or its affiliates. All rights reserved.
 * BGI PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package org.bgi.flexlab.gaea.outputformat.vcf;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.math3.util.FastMath;
import org.bgi.flexlab.gaea.inputformat.positioninformation.PositionInformation;
import org.bgi.flexlab.gaea.utils.SystemConfig;
import org.bgi.flexlab.gaea.utils.math.FisherTest;

/**
 * VCF格式
 * @author ZhangYong
 *
 */
public class VCFFormat {
	/**
	 * 0:SNP; 1:REF; 2:cover but not sure
	 */
	private byte mode;
	
	private String chrName;
	
	private long position;
	
	private String id;
	
	private char refBase;
	
	private String altBases;
	
	private int qual;
	
	private String filters;
	
	private VcfInfo info;
	
	private VcfFormatInfo format;
	
	private static String charSeq = "ACTGNNNN";
	
	/**
	 * AA->0,AC->1,AT->2,AG->3,CA->4,CC->5,CT->6,CG->7,TA->8,TC->9,TT->10,TG->11,GA->12,GC->13,GT->14,GG->15
	 * 实际存储的:0,1,2,3,5,6,7,10,11,15
	 */
	private static byte[] genotypeTable = {0,1,2,3,1,5,6,7,2,6,10,11,3,7,11,15};
	
	private class VcfInfo {
		public String AC;
		public String AF;
		public int DP;
		public double BQ;
		public double SB;
		public double RS;
		public boolean DB;
		
		public VcfInfo() {
			AC = "0";
			AF = "0.0";
			DP = 0;
			BQ = 0.0;
			SB = 0.0;
			RS = 0.0;
			DB = false;
		}
		
		public String toString() {
			StringBuffer infoString = new StringBuffer();
			infoString.append("AC=");
			infoString.append(AC);
			infoString.append(";AF=");
			infoString.append(AF);
			infoString.append(";DP=");
			infoString.append(DP);
			infoString.append(";BQ=");
			infoString.append(BQ);
			infoString.append(";SB=");
			infoString.append(SB);
			infoString.append(";RS=");
			infoString.append(RS);
			if(DB){
				infoString.append(";DB");
			}
			return infoString.toString();
		}
	}
	
	private class VcfFormatInfo {
		public String GT;
		public String AD;
		public String PL;
		public int GQ;
		
		public VcfFormatInfo() {
			GT = "0/0";
			AD = "0,0";
			PL = "0,0,0";
			GQ = 0;
		}
		
		public String toString() {
			StringBuffer formatString = new StringBuffer();
			if(mode == 0) {
				formatString.append("GT:AD:PL:GQ\t");
				formatString.append(GT);
				formatString.append(":");
				formatString.append(AD);
				formatString.append(":");
				formatString.append(PL);
				formatString.append(":");
				formatString.append(GQ);
			} else {
				formatString.append("GT\t");
				formatString.append("0/0");
			}
			return formatString.toString();
		}
	}
	
	public VCFFormat() {
		this((byte)2);
	}

	public VCFFormat(byte mode) {
		this.mode = mode;
		chrName = "";
		position = -1;
		id = ".";
		refBase = 'N';
		altBases = "N";
		qual = 0;
		filters = ".";
	    info = new VcfInfo();
	    format = new VcfFormatInfo();
	}
	
	public void setVcfAlt(String chrName, byte type, int qual, double RankSumTestValue, PositionInformation posInfo, double[] prob) throws Exception {
		this.chrName = chrName;
		position = posInfo.getPosition() + 1;
		refBase = charSeq.charAt(posInfo.getBase() & 0x7);
		this.qual = qual;
		setVCFAltACAFGT(type, charSeq.charAt(posInfo.getBase() & 0x7));
		setFilters(posInfo, qual);
		setInfo(posInfo, RankSumTestValue);
	
		calBQValue(posInfo.getBaseQualSum(), posInfo.getBaseQualNum());
		calSbPvalue(posInfo.getSbstrandSum(), (posInfo.getBase() & 0x3), SystemConfig.getFastaVabbr(type));
		if(mode == 0) {
			calADValue(altBases, posInfo);
			calPLvaule((byte) (posInfo.getBase() & 0x7), altBases, prob);
		}
	}
	
	private void setInfo(PositionInformation posInfo, double rankSumTestValue){
		info.DP = posInfo.getSeqDepth();
		info.RS = rankSumTestValue;
		if((posInfo.getBase() & 0x8) > 0) {  
			info.DB = true;
		}
	}
	
	private void setFilters(PositionInformation posInfo, int qual){
		StringBuilder filters = new StringBuilder();
		if (posInfo.getSeqDepth() < 4) {
			filters.append("dp4");
		}
		if (qual < 20) {
			if(posInfo.getSeqDepth() < 4) {
				filters.append(";");
			}
			filters.append("q20");
		}
		if (posInfo.getSeqDepth() >= 4 && qual >= 20) {
			filters.append("pass");
		}
		this.filters = filters.toString();
	}
	
	public String toString() {
		StringBuffer vcfString = new StringBuffer();
		
		vcfString.append(chrName);
		vcfString.append("\t");
		vcfString.append(position);
		vcfString.append("\t");
		vcfString.append(id);
		vcfString.append("\t");
		vcfString.append(refBase);
		vcfString.append("\t");
		if(mode < 1) {
			vcfString.append(altBases);
		} else {
			vcfString.append(".");
		}
		vcfString.append("\t");
		vcfString.append(qual);
		vcfString.append("\t");
		vcfString.append(filters);
		vcfString.append("\t");
		vcfString.append(info.toString());
		
		if(mode != 2) {
			vcfString.append("\t");
			vcfString.append(format.toString());
		}
		return vcfString.toString();
	}
	
	/**
	 * 输出VCF头部
	 * @param ref 参考基因组
	 * @param sampleName 样本名
	 * @return vcf头部
	 */
	public static String printVCFHeader(String ref, String sampleName){
		StringBuffer vcfString = new StringBuffer();
		
		//Ԫ����
		vcfString.append("##fileformat=VCFv4.1");
		vcfString.append("\n##fileDate=");
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
		vcfString.append(df.format(new Date()));
		vcfString.append("\n##source=GaeaSNP");
		vcfString.append("\n##reference=");
		vcfString.append(ref);
		vcfString.append("\n##phasing=No");
		
		//INFO
		vcfString.append("\n##INFO=<ID=AC,Number=A,Type=Integer,Description=\"Allele count in genotypes, for each ALT allele, in the same order as listed\">");
		vcfString.append("\n##INFO=<ID=AF,Number=A,Type=Float,Description=\"Allele Frequency, for each ALT allele, in the same order as listed\">");
		vcfString.append("\n##INFO=<ID=DP,Number=1,Type=Integer,Description=\"unique reads depth\">");
		vcfString.append("\n##INFO=<ID=BQ,Number=1,Type=Float,Description=\"RMS base quality at this position\">");
		vcfString.append("\n##INFO=<ID=SB,Number=1,Type=Float,Description=\"Strand Bias\">");
		vcfString.append("\n##INFO=<ID=RS,Number=1,Type=Float,Description=\"P-value of rank sum test\">");
		vcfString.append("\n##INFO=<ID=DB,Number=0,Type=Flag,Description=\"dbSNP membership\">");
		
		//filter
		vcfString.append("\n##FILTER=<ID=q20,Description=\"Quality below 20\">");
		vcfString.append("\n##FILTER=<ID=dp4,Description=\"Depth below 4\">");
		
		//FORMAT
		vcfString.append("\n##FORMAT<ID=AD,Number=.,Type=Integer,Description=\"Allelic depths for the ref and alt alleles in the order listed\">");
		vcfString.append("\n##FORMAT<ID=PL,Number=G,Type=Integer,Description=\"Normalized, Phred-scaled likelihoods for genotypes as defined in the VCF specification\">");
		vcfString.append("\n##FORMAT<ID=GT,Number=1,Type=String,Description=\"Genotype\">");
		vcfString.append("\n##FORMAT<ID=GQ,Number=1,Type=Integer,Description=\"Genotype Quality\">");
		vcfString.append("\n#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\t");
		
		if(sampleName != null){
			vcfString.append(sampleName);
		}
		return vcfString.toString(); 
	}

	/**
	 * VCF获取AC,AF,GTֵ
	 * @param
	 */
	public void setVCFAltACAFGT(byte type, char base) {
		String altBase = SystemConfig.getFastaVabbr(type);
		if(altBase.length() > 1) {
			int i;
			if((i = altBase.indexOf(String.valueOf(base))) != -1) {
				if(i == 0) {
					setVCFAltACAFGT(String.valueOf(altBase.charAt(2)), "1", "0.5", "0/1");
				} else {
					setVCFAltACAFGT(String.valueOf(altBase.charAt(0)), "1", "0.5", "0/1");
				}
			} else {
				setVCFAltACAFGT(altBase, "1,1", "0.5,0.5", "1/2");
			}
		} else {
			setVCFAltACAFGT(altBase, "2", "1", "1/1");
		}
	}

	public void setVCFAltACAFGT(String altBase, String AC, String AF, String GT){
		altBases = altBase;
		info.AC = AC;
		info.AF = AF;
		format.GT = GT;
	}
	/**
	 * 计算BQValue
	 * @return BQValue
	 */
	public void calBQValue(long baseQualSum, int baseQualNum) {
		if(baseQualNum == 0) {
			info.BQ  = 0;
		}
		
		double baseQualValue;
		baseQualValue = Math.sqrt(baseQualSum/(double)baseQualNum); 
		info.BQ = baseQualValue;
	}

	/**
	 * SB检验
	 * @param ref 该碱基位点编码值
	 * @param alter 变异基因型
	 * @return sbPvalueֵ SB检验P值
	 */
	public void calSbPvalue(int[] sbstrand, int ref, String alter) {
		FisherTest fisher = new FisherTest();
		double sbPvalue;
		fisher.FisherTestInit(sbstrand, ref, alter);
		sbPvalue = fisher.fisher();
		if (sbPvalue > 1.0) {
			sbPvalue = 1.0d;
		}
		info.SB = sbPvalue;
	}
	
	public void calADValue(String alt, PositionInformation posInfo) throws Exception {
		StringBuilder ADvalue = new StringBuilder();
		String[] altBases = alt.split(",");
		if(altBases.length != 2 && altBases.length != 1) {
			throw new Exception("wrong alt base when cal ad vaule!");
		}
		
		ADvalue.append(posInfo.getAllReadBaseSum(posInfo.getBase() & 0x7));
		ADvalue.append(",");
		ADvalue.append(posInfo.getAllReadBaseSum((altBases[0].charAt(0) >> 1) & 0x3));

		if(altBases.length == 2) {
			ADvalue.append(",");
			ADvalue.append(posInfo.getAllReadBaseSum((altBases[1].charAt(0) >> 1) & 0x3));
		}
		
		format.AD = ADvalue.toString();
	}
	
	/**
	 * 计算PL值
	 * @param ref
	 * @param alt
	 * @param prob
	 * @return
	 * @throws Exception 
	 */
	public void calPLvaule(byte ref, String alt, double[] prob) throws Exception {
		StringBuilder PLvalue = new StringBuilder();
		String[] altBases = alt.split(",");
		if(altBases.length != 2 && altBases.length != 1) {
			throw new Exception("wrong alt base when cal pl vaule!");
		}
		
		byte[] bases = getBases(altBases, ref);
		
		//AA,AB,BB
		//AA,AB,BB,AC,BC,CC
		int[] PLs = new int [getPlNum(altBases)];
		for(int i = 0; i < 2; i++) {
			for(int j = i; j < 2; j++) {
				byte type = (byte) (bases[i] << 2|bases[j]);
				PLs[i * (i + 1)/2 + j] = (int) FastMath.round( -10 * (getConditionalProbabilityByGenotype(prob, type) - getMaxCP(prob)) );
			}
		}
		
		if(getBaseNum(altBases) == 3) {
			for(int i = 0; i < 3; i++) {
				byte type = (byte) (bases[i] << 2|bases[2]);
				
				PLs[3 + i] = (int) FastMath.round( -10 * (getConditionalProbabilityByGenotype(prob, type) - getMaxCP(prob)));
			}
		}
		
		format.PL = toString(getPlNum(altBases), PLvalue, PLs);
		format.GQ = calculateGQ(prob);
	}
	
	private int getBaseNum(String[] altBases) {
		return altBases.length == 2 ? 3 : 2;
	}
	
	private int getPlNum(String[] altBases) {
		return altBases.length == 2 ? 6 : 3;
	}
	
	private byte[] getBases(String[] altBases, byte ref) {
		byte[] bases = new byte[3];
		bases[0] = ref;
		bases[1] = (byte) ((altBases[0].charAt(0) >> 1) & 0x3);
		if(altBases.length == 2) {
			bases[2] = (byte) ((altBases[1].charAt(0) >> 1) & 0x3);
		}
		return bases;
	}
	
	private double getMaxCP(double[] prob) {
		double maxCP = Double.NEGATIVE_INFINITY;
		for(double value : prob) {
			if(value == 0.0) {
				continue;
			}
			if(value > maxCP) {
				maxCP = value;
			}
		}
		return maxCP;
	}
	
	private String toString(int plNum, StringBuilder PLvalue, int[] PLs) {
		for(int i = 0; i < plNum; i++) {
			if(PLs[i] > Short.MAX_VALUE) {
				PLs[i] = Short.MAX_VALUE;
			}
			
			if(i != plNum - 1) {
				PLvalue.append(PLs[i]);
				PLvalue.append(",");
			}
		}
		PLvalue.append(PLs[plNum - 1]);
		return PLvalue.toString();
	}
	
	public double[] get12Max(double[] prob) {
		double[] max12 = new double[2];
		max12[0] = Double.NEGATIVE_INFINITY;
		max12[1] = Double.NEGATIVE_INFINITY;
		for(int i = 0; i < prob.length; i++) {
			if(prob[i] == 0.0) {
				continue;
			}
			if(prob[i] > max12[0]) {
				max12[0] = prob[i];
			} 
			if(prob[i] > max12[1]) {
				max12[1] = prob[i];
			}
		}
		if(max12[1] == Double.NEGATIVE_INFINITY) {
			max12[1] = max12[0];
		}
		return max12;
	}
	
	private int calculateGQ(double[] prob) {
		double[] max12 = get12Max(prob);
		int genotypeQual = (int) Math.round(10* (max12[0] - max12[1]));
		if(genotypeQual > 99) {
			genotypeQual = 99;
		}
		return genotypeQual;
	}
	
	public double getConditionalProbabilityByGenotype(double[] conditionalProbability, byte type) {
		byte index = genotypeTable[type];
		return conditionalProbability[index];
	}
}
