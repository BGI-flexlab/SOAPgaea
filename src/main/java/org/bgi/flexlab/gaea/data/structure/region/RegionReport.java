package org.bgi.flexlab.gaea.data.structure.region;

import java.math.RoundingMode;
import java.text.DecimalFormat;

import org.bgi.flexlab.gaea.data.structure.region.BasicRegion;

public class RegionReport {
	private BasicRegion region;
	
	private int targetMappedReadsNum;
	
	private int targetMappedUniqReadsNum;
	
	private int FlankMappedReadsNum;
	
	private int coveredTargetBaseNum;
	
	private int coveredTargetBase4XNum;
	
	private int coveredTargetBase10XNum;
	
	private int coveredTargetBase20XNum;
	
	private int coveredTargetBase30XNum;
	
	private int coveredTargetBase50XNum;
	
	private int coveredTargetBase100XNum;
	
	private int coveredFlankingBaseNum;
	
	private int coveredFlankingBase4XNum;
	
	private int coveredFlankingBase10XNum;
	
	private int coveredFlankingBase20XNum;
	
	private int coveredFlankingBase30XNum;
	
	private int coveredFlankingBase50XNum;
	
	private int coveredFlankingBase100XNum;
	
	private long targetDeepth;
	
	private long flankingDeepth;
	
	private int coveredChrXTargetBaseNum;
	
	private int coveredChrYTargetBaseNum;
	
	private long targetChrXDeepth;
	
	private long targetChrYDeepth;
	
	private int coveredTargetBase4XWithoutPCRdupNum;
	
	private int coveredTargetBase10XWithoutPCRdupNum;
	
	private int coveredTargetBase20XWithoutPCRdupNum;
	
	private int coveredTargetBase30XWithoutPCRdupNum;
	
	private int coveredTargetBase50XWithoutPCRdupNum;
	
	private int coveredTargetBase100XWithoutPCRdupNum;
	
	private long targetDeepthWithoutPCRdup;
	
	private int coveredFlankingBase4XWithoutPCRdupNum;
	
	private int coveredFlankingBase10XWithoutPCRdupNum;
	
	private int coveredFlankingBase20XWithoutPCRdupNum;
	
	private int coveredFlankingBase30XWithoutPCRdupNum;
	
	private int coveredFlankingBase50XWithoutPCRdupNum;
	
	private int coveredFlankingBase100XWithoutPCRdupNum;
	
	private long flankingDeepthWithoutPCRdup;
	
	private int coveredTargetBaseNumWithoutPCRdup;
	
	private int coveredFlankingBaseNumWithoutPCRdup;
	
	private Sex gender;
	
	public enum Sex{
		M,
		F,
		unKown
	}
	
	public RegionReport() {
		targetMappedReadsNum = 0;
		targetMappedUniqReadsNum = 0;
		FlankMappedReadsNum = 0;
		
		coveredTargetBaseNum = 0;
		coveredTargetBase4XNum = 0;
		coveredTargetBase10XNum = 0;
		coveredTargetBase20XNum = 0;
		coveredTargetBase30XNum = 0;
		coveredTargetBase50XNum = 0;
		coveredTargetBase100XNum = 0;
		coveredFlankingBaseNum = 0;
		coveredFlankingBase4XNum = 0;
		coveredFlankingBase10XNum = 0;
		coveredFlankingBase20XNum = 0;
		coveredFlankingBase30XNum = 0;
		coveredFlankingBase50XNum = 0;
		coveredFlankingBase100XNum = 0;
		targetDeepth = 0;
		flankingDeepth = 0;
		coveredChrXTargetBaseNum = 0;
		coveredChrYTargetBaseNum = 0;
		targetChrXDeepth = 0;
		targetChrYDeepth = 0;
		
		coveredTargetBase4XWithoutPCRdupNum = 0;	
		coveredTargetBase10XWithoutPCRdupNum = 0;	
		coveredTargetBase20XWithoutPCRdupNum = 0;
		coveredTargetBase30XWithoutPCRdupNum = 0;	
		coveredTargetBase50XWithoutPCRdupNum = 0;	
		coveredTargetBase100XWithoutPCRdupNum = 0;	
		targetDeepthWithoutPCRdup = 0;		
		coveredFlankingBase4XWithoutPCRdupNum = 0;	
		coveredFlankingBase10XWithoutPCRdupNum = 0;	
		coveredFlankingBase20XWithoutPCRdupNum = 0;
		coveredFlankingBase30XWithoutPCRdupNum = 0;	
		coveredFlankingBase50XWithoutPCRdupNum = 0;	
		coveredFlankingBase100XWithoutPCRdupNum = 0;	
		flankingDeepthWithoutPCRdup = 0;
		
		region = null;
	}
	public RegionReport(BasicRegion region) {
		targetMappedReadsNum = 0;
		targetMappedUniqReadsNum = 0;
		FlankMappedReadsNum = 0;
		
		coveredTargetBaseNum = 0;
		coveredTargetBase4XNum = 0;
		coveredTargetBase10XNum = 0;
		coveredTargetBase20XNum = 0;
		coveredTargetBase30XNum = 0;
		coveredTargetBase50XNum = 0;
		coveredTargetBase100XNum = 0;
		coveredFlankingBaseNum = 0;
		coveredFlankingBase4XNum = 0;
		coveredFlankingBase10XNum = 0;
		coveredFlankingBase20XNum = 0;
		coveredFlankingBase30XNum = 0;
		coveredFlankingBase50XNum = 0;
		coveredFlankingBase100XNum = 0;
		targetDeepth = 0;
		flankingDeepth = 0;
		coveredChrXTargetBaseNum = 0;
		coveredChrYTargetBaseNum = 0;
		targetChrXDeepth = 0;
		targetChrYDeepth = 0;
		
		coveredTargetBase4XWithoutPCRdupNum = 0;	
		coveredTargetBase10XWithoutPCRdupNum = 0;	
		coveredTargetBase20XWithoutPCRdupNum = 0;
		coveredTargetBase30XWithoutPCRdupNum = 0;	
		coveredTargetBase50XWithoutPCRdupNum = 0;	
		coveredTargetBase100XWithoutPCRdupNum = 0;	
		targetDeepthWithoutPCRdup = 0;		
		coveredFlankingBase4XWithoutPCRdupNum = 0;	
		coveredFlankingBase10XWithoutPCRdupNum = 0;	
		coveredFlankingBase20XWithoutPCRdupNum = 0;
		coveredFlankingBase30XWithoutPCRdupNum = 0;	
		coveredFlankingBase50XWithoutPCRdupNum = 0;	
		coveredFlankingBase100XWithoutPCRdupNum = 0;	
		flankingDeepthWithoutPCRdup = 0;
		
		this.region = region;
	}
	
	public String toReducerString() {
		StringBuffer regionString = new StringBuffer();
		regionString.append("Target Information:\n");
		regionString.append(targetMappedReadsNum);
		regionString.append("\t");
		regionString.append(targetMappedUniqReadsNum);
		regionString.append("\t");
		regionString.append(FlankMappedReadsNum);
		regionString.append("\t");
		regionString.append(coveredTargetBaseNum);
		regionString.append("\t");
		regionString.append(coveredTargetBase4XNum);
		regionString.append("\t");
		regionString.append(coveredTargetBase10XNum);
		regionString.append("\t");
		regionString.append(coveredTargetBase20XNum);
		regionString.append("\t");
		regionString.append(coveredTargetBase30XNum);
		regionString.append("\t");
		regionString.append(coveredTargetBase50XNum);
		regionString.append("\t");
		regionString.append(coveredTargetBase100XNum);
		regionString.append("\t");
		regionString.append(coveredFlankingBaseNum);
		regionString.append("\t");
		regionString.append(coveredFlankingBase4XNum);
		regionString.append("\t");
		regionString.append(coveredFlankingBase10XNum);
		regionString.append("\t");
		regionString.append(coveredFlankingBase20XNum);
		regionString.append("\t");
		regionString.append(coveredFlankingBase30XNum);
		regionString.append("\t");
		regionString.append(coveredFlankingBase50XNum);
		regionString.append("\t");
		regionString.append(coveredFlankingBase100XNum);
		regionString.append("\t");
		regionString.append(targetDeepth);
		regionString.append("\t");
		regionString.append(flankingDeepth);
		regionString.append("\t");
		regionString.append(coveredChrXTargetBaseNum);
		regionString.append("\t");
		regionString.append(coveredChrYTargetBaseNum);
		regionString.append("\t");
		regionString.append(targetChrXDeepth);
		regionString.append("\t");
		regionString.append(targetChrYDeepth);
		regionString.append("\t");
		regionString.append(coveredTargetBaseNumWithoutPCRdup);
		regionString.append("\t");
		regionString.append(coveredTargetBase4XWithoutPCRdupNum);
		regionString.append("\t");
		regionString.append(coveredTargetBase10XWithoutPCRdupNum);
		regionString.append("\t");
		regionString.append(coveredTargetBase20XWithoutPCRdupNum);
		regionString.append("\t");
		regionString.append(coveredTargetBase30XWithoutPCRdupNum);
		regionString.append("\t");
		regionString.append(coveredTargetBase50XWithoutPCRdupNum);
		regionString.append("\t");
		regionString.append(coveredTargetBase100XWithoutPCRdupNum);
		regionString.append("\t");
		regionString.append(targetDeepthWithoutPCRdup);
		regionString.append("\t");
		regionString.append(coveredFlankingBaseNumWithoutPCRdup);
		regionString.append("\t");
		regionString.append(coveredFlankingBase4XWithoutPCRdupNum);
		regionString.append("\t");
		regionString.append(coveredFlankingBase10XWithoutPCRdupNum);
		regionString.append("\t");
		regionString.append(coveredFlankingBase20XWithoutPCRdupNum);
		regionString.append("\t");
		regionString.append(coveredFlankingBase30XWithoutPCRdupNum);
		regionString.append("\t");
		regionString.append(coveredFlankingBase50XWithoutPCRdupNum);
		regionString.append("\t");
		regionString.append(coveredFlankingBase100XWithoutPCRdupNum);
		regionString.append("\t");
		regionString.append(flankingDeepthWithoutPCRdup);
		regionString.append("\n");

		
		return regionString.toString();
	}
	
	public boolean ParseRegionInfo(String line) {
		String[] splitArray = line.split("\t");
		if(splitArray.length < 13){
			return false;
		}
		int i = 0;
		targetMappedReadsNum += Integer.parseInt(splitArray[i++]);
		targetMappedUniqReadsNum += Integer.parseInt(splitArray[i++]);
		FlankMappedReadsNum += Integer.parseInt(splitArray[i++]);
		
		coveredTargetBaseNum += Integer.parseInt(splitArray[i++]);
		coveredTargetBase4XNum += Integer.parseInt(splitArray[i++]);
		coveredTargetBase10XNum += Integer.parseInt(splitArray[i++]);
		coveredTargetBase20XNum += Integer.parseInt(splitArray[i++]);
		coveredTargetBase30XNum += Integer.parseInt(splitArray[i++]);
		coveredTargetBase50XNum += Integer.parseInt(splitArray[i++]);
		coveredTargetBase100XNum += Integer.parseInt(splitArray[i++]);
		coveredFlankingBaseNum += Integer.parseInt(splitArray[i++]);
		coveredFlankingBase4XNum += Integer.parseInt(splitArray[i++]);
		coveredFlankingBase10XNum += Integer.parseInt(splitArray[i++]);
		coveredFlankingBase20XNum += Integer.parseInt(splitArray[i++]);
		coveredFlankingBase30XNum += Integer.parseInt(splitArray[i++]);
		coveredFlankingBase50XNum += Integer.parseInt(splitArray[i++]);
		coveredFlankingBase100XNum += Integer.parseInt(splitArray[i++]);
		targetDeepth += Long.parseLong(splitArray[i++]);
		flankingDeepth += Long.parseLong(splitArray[i++]);
		coveredChrXTargetBaseNum += Integer.parseInt(splitArray[i++]);
		coveredChrYTargetBaseNum += Integer.parseInt(splitArray[i++]);
		targetChrXDeepth += Long.parseLong(splitArray[i++]);;
		targetChrYDeepth += Long.parseLong(splitArray[i++]);;
		
		coveredTargetBaseNumWithoutPCRdup += Integer.parseInt(splitArray[i++]);
		coveredTargetBase4XWithoutPCRdupNum += Integer.parseInt(splitArray[i++]);	
		coveredTargetBase10XWithoutPCRdupNum += Integer.parseInt(splitArray[i++]);	
		coveredTargetBase20XWithoutPCRdupNum += Integer.parseInt(splitArray[i++]);
		coveredTargetBase30XWithoutPCRdupNum += Integer.parseInt(splitArray[i++]);
		coveredTargetBase50XWithoutPCRdupNum += Integer.parseInt(splitArray[i++]);
		coveredTargetBase100XWithoutPCRdupNum += Integer.parseInt(splitArray[i++]);
		targetDeepthWithoutPCRdup += Long.parseLong(splitArray[i++]);		
		coveredFlankingBaseNumWithoutPCRdup += Integer.parseInt(splitArray[i++]);
		coveredFlankingBase4XWithoutPCRdupNum += Integer.parseInt(splitArray[i++]);	
		coveredFlankingBase10XWithoutPCRdupNum += Integer.parseInt(splitArray[i++]);	
		coveredFlankingBase20XWithoutPCRdupNum += Integer.parseInt(splitArray[i++]);
		coveredFlankingBase30XWithoutPCRdupNum += Integer.parseInt(splitArray[i++]);
		coveredFlankingBase50XWithoutPCRdupNum += Integer.parseInt(splitArray[i++]);
		coveredFlankingBase100XWithoutPCRdupNum += Integer.parseInt(splitArray[i++]);
		flankingDeepthWithoutPCRdup += Long.parseLong(splitArray[i++]);
		
		return true;
	}
	
	public String toString(BasicReport basicReport) {
		DecimalFormat df = new DecimalFormat("0.000");
		df.setRoundingMode(RoundingMode.HALF_UP);
		
		StringBuffer regionString = new StringBuffer();
		regionString.append("Target Information:\n");
		regionString.append("Mapped Reads Number in Target:\t");
		regionString.append(this.targetMappedReadsNum);
		regionString.append("\nUniq Mapped Reads Number in Target:\t");
		regionString.append(this.targetMappedUniqReadsNum);
		regionString.append("\nCapture specificity:\t");
		regionString.append(df.format(getCaptureSpecificity(basicReport)));
		regionString.append("%\nCapture Effiency:\t");
		regionString.append(df.format(getCaptureEffiency(basicReport)));
		regionString.append("%\nMapped Reads Number in Flanking:\t");
		regionString.append(this.FlankMappedReadsNum);
		regionString.append("\nTarget size:\t");
		regionString.append(region.getRegionSize());
		regionString.append("\nTarget coverage:\t");
		regionString.append(df.format(getRegionCoverage()));
		regionString.append("%\nTarget coverage > 4X percentage:\t");
		regionString.append(df.format(100 * (this.coveredTargetBase4XNum/(double)region.getRegionSize())));
		regionString.append("%\nTarget coverage > 10X percentage:\t");
		regionString.append(df.format(100 * (this.coveredTargetBase10XNum/(double)region.getRegionSize())));
		regionString.append("%\nTarget coverage > 20X percentage:\t");
		regionString.append(df.format(100 * (this.coveredTargetBase20XNum/(double)region.getRegionSize())));
		regionString.append("%\nTarget coverage > 30X percentage:\t");
		regionString.append(df.format(100 * (this.coveredTargetBase30XNum/(double)region.getRegionSize())));
		regionString.append("%\nTarget coverage > 50X percentage:\t");
		regionString.append(df.format(100 * (this.coveredTargetBase50XNum/(double)region.getRegionSize())));
		regionString.append("%\nTarget coverage > 100X percentage:\t");
		regionString.append(df.format(100 * (this.coveredTargetBase100XNum/(double)region.getRegionSize())));
		regionString.append("%\nTarget Mean Depth:\t");
		regionString.append(df.format(getRegionMeanDepth()));
		regionString.append("\nFlanking size:\t");
		regionString.append(region.getRegionFlankSize());
		regionString.append("\nFlanking coverage:\t");
		regionString.append(df.format(getFlankingCoverage()));
		regionString.append("%\nFlanking coverage > 4X percentage:\t");
		regionString.append(df.format(100 * (this.coveredFlankingBase4XNum/(double)region.getRegionFlankSize())));
		regionString.append("%\nFlanking coverage > 10X percentage:\t");
		regionString.append(df.format(100 * (this.coveredFlankingBase10XNum/(double)region.getRegionFlankSize())));
		regionString.append("%\nFlanking coverage > 20X percentage:\t");
		regionString.append(df.format(100 * (this.coveredFlankingBase20XNum/(double)region.getRegionFlankSize())));
		regionString.append("%\nFlanking coverage > 30X percentage:\t");
		regionString.append(df.format(100 * (this.coveredFlankingBase30XNum/(double)region.getRegionFlankSize())));
		regionString.append("%\nFlanking coverage > 50X percentage:\t");
		regionString.append(df.format(100 * (this.coveredFlankingBase50XNum/(double)region.getRegionFlankSize())));
		regionString.append("%\nFlanking coverage > 100X percentage:\t");
		regionString.append(df.format(100 * (this.coveredFlankingBase100XNum/(double)region.getRegionFlankSize())));
		regionString.append("%\nFlanking Mean Depth:\t");
		regionString.append(df.format(getFlankingMeanDepth()));
		
		regionString.append("\nTarget coverage[RM DUP]:\t");
		regionString.append(df.format(getRegionCoverageWithoutDUP()));
		regionString.append("%\nTarget coverage > 4X percentage[RM DUP]:\t");
		regionString.append(df.format(100 * (this.coveredTargetBase4XWithoutPCRdupNum/(double)region.getRegionSize())));
		regionString.append("%\nTarget coverage > 10X percentage[RM DUP]:\t");
		regionString.append(df.format(100 * (this.coveredTargetBase10XWithoutPCRdupNum/(double)region.getRegionSize())));
		regionString.append("%\nTarget coverage > 20X percentage[RM DUP]:\t");
		regionString.append(df.format(100 * (this.coveredTargetBase20XWithoutPCRdupNum/(double)region.getRegionSize())));
		regionString.append("%\nTarget coverage > 30X percentage[RM DUP]:\t");
		regionString.append(df.format(100 * (this.coveredTargetBase30XWithoutPCRdupNum/(double)region.getRegionSize())));
		regionString.append("%\nTarget coverage > 50X percentage[RM DUP]:\t");
		regionString.append(df.format(100 * (this.coveredTargetBase50XWithoutPCRdupNum/(double)region.getRegionSize())));
		regionString.append("%\nTarget coverage > 100X percentage[RM DUP]:\t");
		regionString.append(df.format(100 * (this.coveredTargetBase100XWithoutPCRdupNum/(double)region.getRegionSize())));
		regionString.append("%\nTarget Mean Depth[RM DUP]:\t");
		regionString.append(df.format(getRegionMeanDepthWithoutDup()));
		regionString.append("\nFlanking coverage[RM DUP]:\t");
		regionString.append(df.format(getFlankingCoverageWithoutDUP()));
		regionString.append("%\nFlanking coverage > 4X percentage[RM DUP]:\t");
		regionString.append(df.format(100 * (this.coveredFlankingBase4XWithoutPCRdupNum/(double)region.getRegionFlankSize())));
		regionString.append("%\nFlanking coverage > 10X percentage[RM DUP]:\t");
		regionString.append(df.format(100 * (this.coveredFlankingBase10XWithoutPCRdupNum/(double)region.getRegionFlankSize())));
		regionString.append("%\nFlanking coverage > 20X percentage[RM DUP]:\t");
		regionString.append(df.format(100 * (this.coveredFlankingBase20XWithoutPCRdupNum/(double)region.getRegionFlankSize())));
		regionString.append("%\nFlanking coverage > 30X percentage[RM DUP]:\t");
		regionString.append(df.format(100 * (this.coveredFlankingBase30XWithoutPCRdupNum/(double)region.getRegionFlankSize())));
		regionString.append("%\nFlanking coverage > 50X percentage[RM DUP]:\t");
		regionString.append(df.format(100 * (this.coveredFlankingBase50XWithoutPCRdupNum/(double)region.getRegionFlankSize())));
		regionString.append("%\nFlanking coverage > 100X percentage[RM DUP]:\t");
		regionString.append(df.format(100 * (this.coveredFlankingBase100XWithoutPCRdupNum/(double)region.getRegionFlankSize())));
		regionString.append("%\nFlanking Mean Depth[RM DUP]:\t");
		regionString.append(df.format(getFlankingMeanDepthWithoutDup()));
		
		regionString.append("\nchrX region coverage:\t");
		regionString.append(df.format(100 * (this.coveredChrXTargetBaseNum/(double)region.getChrSize("chrX"))));
		regionString.append("%\nchrX region depth:\t");
		regionString.append(df.format(this.targetChrXDeepth/(double)this.coveredChrXTargetBaseNum));
		regionString.append("\nchrY region coverage:\t");
		regionString.append(df.format(100 * (this.coveredChrYTargetBaseNum/(double)region.getChrSize("chrY"))));
		regionString.append("%\nchrY region depth:\t");
		regionString.append(df.format(this.targetChrYDeepth/(double)this.coveredChrYTargetBaseNum));
		regionString.append("\npredicted gender:\t");
		regionString.append(gender.toString());
		regionString.append("\n");

		return regionString.toString();
	}
	
	public double getRegionCoverage() {
		return (100 * (this.coveredTargetBaseNum/(double)region.getRegionSize()));
	}
	
	public double getRegionMeanDepth() {
		return (this.targetDeepth/(double)this.coveredTargetBaseNum);
	}
	
	public double getFlankingCoverage() {
		return (100 * (this.coveredFlankingBaseNum/(double) region.getRegionFlankSize()));
	}
	
	public double getFlankingMeanDepth() {
		return (this.flankingDeepth/(double)this.coveredFlankingBaseNum);
	}
	
	public double getCaptureSpecificity(BasicReport basicReport) {
		return 100 * (this.targetMappedUniqReadsNum / (double) basicReport.getUniqMappedReadsNum());
	}
	
	public double getCaptureEffiency(BasicReport basicReport) {
		return 100 * (this.targetDeepth / (double) basicReport.getTotalBaseNum());
	}
	public double getRegionMeanDepthWithoutDup() {
		return this.targetDeepthWithoutPCRdup / (double) this.coveredTargetBaseNumWithoutPCRdup;
	}
	
	public double getFlankingMeanDepthWithoutDup() {
		return (this.flankingDeepthWithoutPCRdup/(double)this.coveredFlankingBaseNumWithoutPCRdup);
	}
	
	public double getRegionCoverageWithoutDUP() {
		return (100 * (this.coveredTargetBaseNumWithoutPCRdup/(double)region.getRegionSize()));
	}
	public double getFlankingCoverageWithoutDUP() {
		return (100 * (this.coveredFlankingBaseNumWithoutPCRdup/(double) region.getRegionFlankSize()));
	}
	
	public void setPredictedGender(Sex gender) {
		this.gender = gender;
	}
	
	public Sex getPredictedGender() {
		return this.gender;
	}
	
	/**
	 * targetMappedReadsNum
	 * @param i
	 */
	public void targetMappedReadsNumIncrease(int i) {
		targetMappedReadsNum += i;
	}
	
	public int getTargetMappedReadsNum() {
		return this.targetMappedReadsNum;
	}
	
	/**
	 * targetMappedUniqReadsNum
	 * @param i
	 */
	public void targetMappedUniqReadsNumIncrease(int i) {
		targetMappedUniqReadsNum += i;
	}
	
	public int getTargetMappedUniqReadsNum() {
		return this.targetMappedUniqReadsNum;
	}
	
	/**
	 * FlankMappedReadsNum
	 * @param i
	 */
	public void FlankMappedReadsNumIncrease(int i) {
		FlankMappedReadsNum += i;
	}
	
	public int getFlankMappedReadsNum() {
		return this.FlankMappedReadsNum;
	}
	
	public void coveredTargetBaseNumIncrease(int i) {
		coveredTargetBaseNum += i;
	}
	
	public long getCoveredTargetBaseNum() {
		return this.coveredTargetBaseNum;
	}
	
	public void coveredTargetBase4XNumIncrease(int i) {
		coveredTargetBase4XNum += i;
	}
	
	public long getCoveredTargetBase4XNum() {
		return this.coveredTargetBase4XNum;
	}
	
	public void coveredFlankingBaseNumIncrease(int i) {
		coveredFlankingBaseNum += i;
	}
	
	public long getCoveredFlankingBaseNum() {
		return this.coveredFlankingBaseNum;
	}
	
	public void coveredFlankingBase4XNumIncrease(int i) {
		coveredFlankingBase4XNum += i;
	}
	
	public long getCoveredFlankingBase4XNum() {
		return this.coveredFlankingBase4XNum;
	}
	
	public void coveredFlankingBase10XNumIncrease(int i) {
		coveredFlankingBase10XNum += i;
	}
	
	public long getCoveredFlankingBase10XNum() {
		return this.coveredFlankingBase10XNum;
	}
	
	public void coveredFlankingBase20XNumIncrease(int i) {
		coveredFlankingBase20XNum += i;
	}
	
	public long getCoveredFlankingBase20XNum() {
		return this.coveredFlankingBase20XNum;
	}
	
	public long getCoveredFlankingBase30XNum() {
		return this.coveredFlankingBase30XNum;
	}
	
	public void coveredFlankingBase30XNumIncrease(int i) {
		coveredFlankingBase30XNum += i;
	}
	
	public long getCoveredFlankingBase50XNum() {
		return this.coveredFlankingBase50XNum;
	}
	
	public void coveredFlankingBase50XNumIncrease(int i) {
		coveredFlankingBase50XNum += i;
	}
	
	public long getCoveredFlankingBase100XNum() {
		return this.coveredFlankingBase100XNum;
	}
	
	public void coveredFlankingBase100XNumIncrease(int i) {
		coveredFlankingBase100XNum += i;
	}
	
	public void coveredTargetBase20XNumIncrease(int i) {
		coveredTargetBase20XNum += i;
	}
	
	public long getCoveredTargetBase20XNum() {
		return this.coveredTargetBase20XNum;
	}
	
	public void coveredTargetBase30XNumIncrease(int i) {
		coveredTargetBase30XNum += i;
	}
	
	public long getCoveredTargetBase30XNum() {
		return this.coveredTargetBase30XNum;
	}
	
	public void coveredTargetBase50XNumIncrease(int i) {
		coveredTargetBase50XNum += i;
	}
	
	public long getCoveredTargetBase50XNum() {
		return this.coveredTargetBase50XNum;
	}
	
	public void coveredTargetBase100XNumIncrease(int i) {
		coveredTargetBase100XNum += i;
	}
	
	public long getCoveredTargetBase100XNum() {
		return this.coveredTargetBase100XNum;
	}
	
	public void coveredTargetBase10XNumIncrease(int i) {
		coveredTargetBase10XNum += i;
	}
	
	public long getCoveredTargetBase10XNum() {
		return this.coveredTargetBase10XNum;
	}
	
	public void targetDeepthIncrease(long i) {
		targetDeepth += i;
	}
	
	public long getTargetDeepth() {
		return this.targetDeepth;
	}
	
	public void flankingDeepthIncrease(long i) {
		flankingDeepth += i;
	}
	
	public long getFlankingDeepth() {
		return this.flankingDeepth;
	}
	
	public void coveredChrXTargetBaseNumIncrease(int i) {
		coveredChrXTargetBaseNum += i;
	}
	
	public int getCoveredChrXTargetBaseNum() {
		return coveredChrXTargetBaseNum;
	}
	
	public void coveredChrYTargetBaseNumIncrease(int i) {
		coveredChrYTargetBaseNum += i;
	}
	
	public int getCoveredChrYTargetBaseNum() {
		return coveredChrYTargetBaseNum;
	}
	
	public void targetChrXDeepthIncrease(long i) {
		targetChrXDeepth += i;
	}
	
	public long getTargetChrXDeepth() {
		return this.targetChrXDeepth;
	}
	
	public void targetChrYDeepthIncrease(long i) {
		targetChrYDeepth += i;
	}
	
	public long getTargetChrYDeepth() {
		return this.targetChrYDeepth;
	}
	/**
	 * @return the coveredTargetBase4XWithoutPCRdupNum
	 */
	public int getCoveredTargetBase4XWithoutPCRdupNum() {
		return coveredTargetBase4XWithoutPCRdupNum;
	}
	/**
	 * @param coveredTargetBase4XWithoutPCRdupNum the coveredTargetBase4XWithoutPCRdupNum to set
	 */
	public void CoveredTargetBase4XWithoutPCRdupNumIncrease(
			int coveredTargetBase4XWithoutPCRdupNum) {
		this.coveredTargetBase4XWithoutPCRdupNum += coveredTargetBase4XWithoutPCRdupNum;
	}
	/**
	 * @return the coveredTargetBase10XWithoutPCRdupNum
	 */
	public int getCoveredTargetBase10XWithoutPCRdupNum() {
		return coveredTargetBase10XWithoutPCRdupNum;
	}
	/**
	 * @param coveredTargetBase10XWithoutPCRdupNum the coveredTargetBase10XWithoutPCRdupNum to set
	 */
	public void CoveredTargetBase10XWithoutPCRdupNumIncrease(
			int coveredTargetBase10XWithoutPCRdupNum) {
		this.coveredTargetBase10XWithoutPCRdupNum += coveredTargetBase10XWithoutPCRdupNum;
	}
	/**
	 * @return the coveredTargetBase20XWithoutPCRdupNum
	 */
	public int getCoveredTargetBase20XWithoutPCRdupNum() {
		return coveredTargetBase20XWithoutPCRdupNum;
	}
	/**
	 * @param coveredTargetBase20XWithoutPCRdupNum the coveredTargetBase20XWithoutPCRdupNum to set
	 */
	public void CoveredTargetBase20XWithoutPCRdupNumIncrease(
			int coveredTargetBase20XWithoutPCRdupNum) {
		this.coveredTargetBase20XWithoutPCRdupNum += coveredTargetBase20XWithoutPCRdupNum;
	}
	
	public int getCoveredTargetBase30XWithoutPCRdupNum() {
		return coveredTargetBase30XWithoutPCRdupNum;
	}
	
	public void CoveredTargetBase30XWithoutPCRdupNumIncrease(
			int coveredTargetBase30XWithoutPCRdupNum) {
		this.coveredTargetBase30XWithoutPCRdupNum += coveredTargetBase30XWithoutPCRdupNum;
	}
	
	public int getCoveredTargetBase50XWithoutPCRdupNum() {
		return coveredTargetBase50XWithoutPCRdupNum;
	}
	
	public void CoveredTargetBase50XWithoutPCRdupNumIncrease(
			int coveredTargetBase50XWithoutPCRdupNum) {
		this.coveredTargetBase50XWithoutPCRdupNum += coveredTargetBase50XWithoutPCRdupNum;
	}
	
	public int getCoveredTargetBase100XWithoutPCRdupNum() {
		return coveredTargetBase100XWithoutPCRdupNum;
	}
	
	public void CoveredTargetBase100XWithoutPCRdupNumIncrease(
			int coveredTargetBase20XWithoutPCRdupNum) {
		this.coveredTargetBase100XWithoutPCRdupNum += coveredTargetBase20XWithoutPCRdupNum;
	}
	
	
	/**
	 * @return the targetDeepthWithoutPCRdup
	 */
	public long getTargetDeepthWithoutPCRdup() {
		return targetDeepthWithoutPCRdup;
	}
	/**
	 * @param targetDeepthWithoutPCRdup the targetDeepthWithoutPCRdup to set
	 */
	public void TargetDeepthWithoutPCRdupIncrease(long targetDeepthWithoutPCRdup) {
		this.targetDeepthWithoutPCRdup += targetDeepthWithoutPCRdup;
	}
	/**
	 * @return the coveredFlankingBase4XWithoutPCRdupNum
	 */
	public int getCoveredFlankingBase4XWithoutPCRdupNum() {
		return coveredFlankingBase4XWithoutPCRdupNum;
	}
	/**
	 * @param coveredFlankingBase4XWithoutPCRdupNum the coveredFlankingBase4XWithoutPCRdupNum to set
	 */
	public void CoveredFlankingBase4XWithoutPCRdupNumIncrease(
			int coveredFlankingBase4XWithoutPCRdupNum) {
		this.coveredFlankingBase4XWithoutPCRdupNum += coveredFlankingBase4XWithoutPCRdupNum;
	}
	/**
	 * @return the coveredFlankingBase10XWithoutPCRdupNum
	 */
	public int getCoveredFlankingBase10XWithoutPCRdupNum() {
		return coveredFlankingBase10XWithoutPCRdupNum;
	}
	/**
	 * @param coveredFlankingBase10XWithoutPCRdupNum the coveredFlankingBase10XWithoutPCRdupNum to set
	 */
	public void CoveredFlankingBase10XWithoutPCRdupNumIncrease(
			int coveredFlankingBase10XWithoutPCRdupNum) {
		this.coveredFlankingBase10XWithoutPCRdupNum += coveredFlankingBase10XWithoutPCRdupNum;
	}
	/**
	 * @return the coveredFlankingBase20XWithoutPCRdupNum
	 */
	public int getCoveredFlankingBase20XWithoutPCRdupNum() {
		return coveredFlankingBase20XWithoutPCRdupNum;
	}
	/**
	 * @param coveredFlankingBase20XWithoutPCRdupNum the coveredFlankingBase20XWithoutPCRdupNum to set
	 */
	public void CoveredFlankingBase20XWithoutPCRdupNumIncrease(
			int coveredFlankingBase20XWithoutPCRdupNum) {
		this.coveredFlankingBase20XWithoutPCRdupNum += coveredFlankingBase20XWithoutPCRdupNum;
	}
	
	public int getCoveredFlankingBase30XWithoutPCRdupNum() {
		return coveredFlankingBase30XWithoutPCRdupNum;
	}

	public void CoveredFlankingBase30XWithoutPCRdupNumIncrease(
			int coveredFlankingBase30XWithoutPCRdupNum) {
		this.coveredFlankingBase30XWithoutPCRdupNum += coveredFlankingBase30XWithoutPCRdupNum;
	}
	
	public int getCoveredFlankingBase50XWithoutPCRdupNum() {
		return coveredFlankingBase50XWithoutPCRdupNum;
	}

	public void CoveredFlankingBase50XWithoutPCRdupNumIncrease(
			int coveredFlankingBase50XWithoutPCRdupNum) {
		this.coveredFlankingBase50XWithoutPCRdupNum += coveredFlankingBase50XWithoutPCRdupNum;
	}
	
	public int getCoveredFlankingBase100XWithoutPCRdupNum() {
		return coveredFlankingBase100XWithoutPCRdupNum;
	}

	public void CoveredFlankingBase100XWithoutPCRdupNumIncrease(
			int coveredFlankingBase100XWithoutPCRdupNum) {
		this.coveredFlankingBase100XWithoutPCRdupNum += coveredFlankingBase100XWithoutPCRdupNum;
	}
	
	
	/**
	 * @return the flankingDeepthWithoutPCRdup
	 */
	public long getFlankingDeepthWithoutPCRdup() {
		return flankingDeepthWithoutPCRdup;
	}
	/**
	 * @param flankingDeepthWithoutPCRdup the flankingDeepthWithoutPCRdup to set
	 */
	public void FlankingDeepthWithoutPCRdupIncrease(long flankingDeepthWithoutPCRdup) {
		this.flankingDeepthWithoutPCRdup += flankingDeepthWithoutPCRdup;
	}
	public int getCoveredTargetBaseNumWithoutPCRdup() {
		return coveredTargetBaseNumWithoutPCRdup;
	}
	public void CoveredTargetBaseNumWithoutPCRdupIncrease(
			int coveredTargetBaseNumWithoutPCRdup) {
		this.coveredTargetBaseNumWithoutPCRdup += coveredTargetBaseNumWithoutPCRdup;
	}
	public int getCoveredFlankingBaseNumWithoutPCRdup() {
		return coveredFlankingBaseNumWithoutPCRdup;
	}
	public void CoveredFlankingBaseNumWithoutPCRdupIncrease(
			int coveredFlankingBaseNumWithoutPCRdup) {
		this.coveredFlankingBaseNumWithoutPCRdup += coveredFlankingBaseNumWithoutPCRdup;
	}
}
