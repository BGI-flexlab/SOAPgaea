package org.bgi.flexlab.gaea.tools.bamqualtiycontrol.counter;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hdfs.server.namenode.UnsupportedActionException;
import org.bgi.flexlab.gaea.tools.bamqualtiycontrol.counter.CounterProperty.BaseType;
import org.bgi.flexlab.gaea.tools.bamqualtiycontrol.counter.CounterProperty.DepthType;
import org.bgi.flexlab.gaea.tools.bamqualtiycontrol.counter.CounterProperty.Interval;
import org.bgi.flexlab.gaea.tools.bamqualtiycontrol.counter.CounterProperty.ReadType;

public class Tracker {
	
	private static String formatKey(CounterProperty... properties ) {
		String key = "";
		for(CounterProperty property : properties)
			key += property;
		return key;
	}
	
	public static class BaseTracker {
		
		private List<BaseCounter> counters = new ArrayList<>();
		
		private Map<String, BaseCounter> mapCounter = new LinkedHashMap<>();
		
		private DepthType depth;
		
		private DepthType noPCRdepth;
				
		private Interval region;
		
		private BaseType type;
				
		private boolean workForRegionReport = false;
		
		public void setTrackerAttribute(Interval region, DepthType depth, DepthType noPCRdepth) {
			workForRegionReport = true;
			this.region = region;
			this.depth = depth;
			this.noPCRdepth = noPCRdepth;
			notifyCounters();
		}
		
		public void setTrackerAttribute(BaseType type) {
			workForRegionReport = false;
			this.type = type;
			notifyCounters();
		}
		
		public void notifyCounters() {
			// TODO Auto-generated method stub
			if(!workForRegionReport)
				for(int i = 0;i < counters.size(); i++){
					counters.get(i).update(type);
				}
			else
				for(int i = 0;i < counters.size(); i++) {
					counters.get(i).update(region, depth, noPCRdepth);
				}
		}

		public void register(BaseCounter o) {
			System.out.println("register");
			counters.add(o);
			mapCounter.put(o.formatKey(), o);
		}
		
		public void register(List<BaseCounter> os) {
			counters.addAll(os);
			for(BaseCounter o : os)
				mapCounter.put(o.formatKey(), o);
		}
		
		public void unregister(BaseCounter o) throws UnsupportedActionException {
			throw new UnsupportedActionException("function isn't supported");
		}
		
		public Map<String, BaseCounter> getCounterMap() {
			return mapCounter;
		}
		
		public long getProperty(CounterProperty... properties) {
			return mapCounter.get(formatKey(properties)).getProperty();
		}
	
	}
	
	public static class ReadsTracker {
				
		private ReadType type;
		
		private Interval interval;
		
		private List<ReadsCounter> counters = new ArrayList<>();
		
		private Map<String, ReadsCounter> mapCounter = new LinkedHashMap<>();
		
		private boolean workForRegionReport = false;
		
		public void setTrackerAttribute(ReadType type) {
			workForRegionReport = false;
			this.type = type;
			notifyCounters();
		}
		
		public void setTrackerAttribute(ReadType type, Interval interval) {
			workForRegionReport = true;
			this.type = type;
			this.interval = interval;
			notifyCounters();
		}
		
		public void register(ReadsCounter counter) {
			counters.add(counter);
			mapCounter.put(counter.formatKey(), counter);
		}
		
		public void register(List<ReadsCounter> counters) {
			this.counters.addAll(counters);
			for(ReadsCounter counter : counters)
				mapCounter.put(counter.formatKey(), counter);
		}
		
		public void unregister(BaseCounter o) throws UnsupportedActionException {
			throw new UnsupportedActionException("function is not supported");
		}
		
		public long getReadsCount(CounterProperty... properties) {
			// TODO Auto-generated method stub
			return mapCounter.get(formatKey(properties)).getReadsCount();
		}
		
		public Map<String, ReadsCounter> getCounterMap() {
			return mapCounter;
		}
		
		public void notifyCounters() {
			if (!workForRegionReport) {
				for(ReadsCounter counter : counters) {
					counter.update(type);
				}
			} else {
				for(ReadsCounter counter : counters)
					counter.update(type, interval);
			}
			
		}
	}

}
