package org.bgi.flexlab.gaea.tools.bamqualtiycontrol.report;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.io.Text;
import org.bgi.flexlab.gaea.data.structure.region.TargetRegion;
import org.bgi.flexlab.gaea.tools.bamqualtiycontrol.report.CounterProperty.ReadType;

public class UnmappedReport {
	
	private Map<String, ArrayList<Long>> unmappedSites = new ConcurrentHashMap<String, ArrayList<Long>>();
	
	private long unmappedEnd = 0, unmappedStart = 0;
	
	private boolean firstUnmappedSite = true;
	
	public String toReducerString() {
		StringBuilder info = new StringBuilder();
		for(String key : unmappedSites.keySet()) {
			ArrayList<Long> unmaped = unmappedSites.get(key);
			for(int i = 0; i < unmaped.size(); i += 2) {
				info.append(unmaped.get(i));
				info.append("\t");
				info.append(unmaped.get(i + 1));
				info.append("\n");
			}
		}
		return info.toString();
	}
	
	public ArrayList<Long> getUnmappedSites(String chrName) {
		if(chrName == null || chrName == "") {
			return null;
		}
		
		if(!unmappedSites.containsKey(chrName)) {
			ArrayList<Long> sites = new ArrayList<Long>();
			unmappedSites.put(chrName, sites);
		}
		
		return unmappedSites.get(chrName);
	}
	
	public boolean constructMapReport(long winNum, String chrName, Iterable<Text> values, BasicReport basicReport) {
		if(winNum < 0 || chrName.equals("-1")) {//unmapped
			Iterator<Text> vals = values.iterator();
			while (vals.hasNext()) {
				basicReport.getReadsTracker().setTrackerAttribute(ReadType.TOTALREADS);
				vals.next();
			}
			return true;
		}
		return false;
	}
	
	public void finalize(ArrayList<Long> unmappedSites) {
		if(unmappedEnd != 0 && unmappedStart != 0) {
			unmappedSites.add(unmappedStart);
			unmappedSites.add(unmappedEnd + 1);
		}
	}
	
	public void updateUnmappedSites(long pos, ArrayList<Long> unmappedSites) {
		if(firstUnmappedSite) {
			unmappedEnd = unmappedStart = pos;
			firstUnmappedSite = false;
		} else {
			if(pos == unmappedEnd + 1) {
				unmappedEnd++;
			} else {
				unmappedSites.add(unmappedStart);
				unmappedSites.add(unmappedEnd + 1);
				unmappedEnd = unmappedStart = pos;
			}
		}
	}

}
