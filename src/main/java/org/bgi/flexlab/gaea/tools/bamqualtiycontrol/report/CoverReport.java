package org.bgi.flexlab.gaea.tools.bamqualtiycontrol.report;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.bgi.flexlab.gaea.data.structure.positioninformation.depth.PositionDepth;
import org.bgi.flexlab.gaea.data.structure.reference.ChromosomeInformationShare;
import org.bgi.flexlab.gaea.tools.bamqualtiycontrol.report.CounterProperty.BaseType;
import org.bgi.flexlab.gaea.tools.bamqualtiycontrol.report.CounterProperty.Depth;
import org.bgi.flexlab.gaea.tools.bamqualtiycontrol.report.CounterProperty.DepthType;
import org.bgi.flexlab.gaea.tools.bamqualtiycontrol.report.CounterProperty.Interval;
import org.bgi.flexlab.gaea.tools.bamqualtiycontrol.report.Tracker.BaseTracker;

public class CoverReport{

	private BaseTracker bTracker;
	
	private ChromosomeInformationShare chrInfo;
	
	private static String chrName;
	
	private static Map<String, CoverReport> coverReports;
	
	private CoverReport(ChromosomeInformationShare chrInfo) {
		super();
		this.chrInfo = chrInfo;
		this.chrName = chrInfo.getChromosomeName();
		coverReports = new ConcurrentHashMap<>();
	}
	
	public void constructDepthReport(PositionDepth deep, int i) {
		int depth = deep.getPosDepth(i);
		if(depth != 0) {
			setBaseTrackerAttribute(Interval.WHOLEGENOME, DepthType.NORMAL.setDepth(depth), DepthType.WITHOUT_PCR);
			setBaseTrackerAttribute(BaseType.COVERED);
			if(deep.hasIndelReads(i)) 
				setBaseTrackerAttribute(BaseType.INDELREF);
			
			if(deep.hasMismatchReads(i)) 
				setBaseTrackerAttribute(BaseType.MISMATCHREF);				
		} else {
			if(deep.isDeletionBaseWithNoConver(i)) 
				setBaseTrackerAttribute(BaseType.COVERED);
		}
	}
	
	public String toReducerString() {
		StringBuffer coverString = new StringBuffer();
		coverString.append("Cover Information:\n");
		coverString.append(chrName);
		coverString.append("\t");
		coverString.append(bTracker.getBaseCount(BaseType.COVERED));
		coverString.append("\t");
		coverString.append(bTracker.getTotalDepth(Interval.WHOLEGENOME, Depth.TOTALDEPTH, DepthType.NORMAL));
		coverString.append("\t");
		coverString.append(bTracker.getBaseCount(BaseType.INDELREF));
		coverString.append("\t");
		coverString.append(bTracker.getBaseCount(BaseType.MISMATCHREF));
		coverString.append("\n");
		
		return coverString.toString();
	}
	
	public String toString() {
		DecimalFormat df = new DecimalFormat("0.000");
		df.setRoundingMode(RoundingMode.HALF_UP);
		
		StringBuffer coverString = new StringBuffer();
		coverString.append("chromsome:\t");
		coverString.append(chrName);
		coverString.append("\nCoverage:\t");
		coverString.append(df.format(getCoverage()));
		coverString.append("%\nMean Depth:\t");
		coverString.append(df.format(getMeanDepth()));
		coverString.append("\nrate of position according to reference that have at least one indel reads support:\t");
		coverString.append(df.format(getRateOf(BaseType.INDELREF)));
		coverString.append("%\nrate of position according to reference that have at least one mismatch reads support:\t");
		coverString.append(df.format(getRateOf(BaseType.MISMATCHREF)));
		coverString.append("%\n\n");
		
		return coverString.toString();
	}
	
	public double getCoverage() {
		return (100 * (bTracker.getBaseCount(BaseType.COVERED)/(double)chrInfo.getLength()));
	}
	
	public double getMeanDepth() {
		return (bTracker.getTotalDepth(Interval.WHOLEGENOME, Depth.TOTALDEPTH, DepthType.NORMAL)/(double)bTracker.getBaseCount(BaseType.COVERED));
	}
	
	public double getRateOf(BaseType type) {
		return (100 * (bTracker.getBaseCount(type)/(double)chrInfo.getLength()));
	}
	
	public void register() {
		bTracker.register(createBaseCounters());
	}
	
	public void setBaseTrackerAttribute(BaseType type) {
		bTracker.setTrackerAttribute(type);
	}
	
	public void setBaseTrackerAttribute(Interval region, DepthType depth, DepthType noPCRdepth) {
		bTracker.setTrackerAttribute(region, depth, noPCRdepth);
	}
	
	public List<BaseCounter> createBaseCounters() {
		List<BaseCounter> counters = new ArrayList<>();
		Collections.addAll(counters, new BaseCounter(Interval.WHOLEGENOME, Depth.TOTALDEPTH, DepthType.NORMAL),
									 new BaseCounter(BaseType.COVERED),
									 new BaseCounter(BaseType.INDELREF),
									 new BaseCounter(BaseType.MISMATCHREF));
		return counters;
	}
	
	public static Map<String, CoverReport>getCoverReports() {
		return coverReports;
	}
	
	public static CoverReport getCoverReport(ChromosomeInformationShare chrInfo) {
		if(!coverReports.containsKey(chrInfo.getChromosomeName())) {
			CoverReport coverInfo = new CoverReport(chrInfo);
			coverReports.put(chrInfo.getChromosomeName(), coverInfo);
		}
		return coverReports.get(chrInfo.getChromosomeName());
	}
	
	public static CoverReport getCoverReport(String chrName) {
		return coverReports.get(chrName);
	}
	
	public static void addCoverReport(ChromosomeInformationShare chrInfo) {
		CoverReport coverInfo = new CoverReport(chrInfo);
		coverReports.put(chrName, coverInfo);
	}

}
