package org.bgi.flexlab.gaea.tools.bamqualtiycontrol.report;

import org.bgi.flexlab.gaea.tools.bamqualtiycontrol.report.CounterProperty.Interval;
import org.bgi.flexlab.gaea.tools.bamqualtiycontrol.report.CounterProperty.ReadType;

public class ReadsCounter {
	
	private long count;
	
	private ReadType type;
	
	private Interval interval;
	
	private CounterProperty[] properties;

	public ReadsCounter(CounterProperty... counterProperties) {
		properties = counterProperties;
		if(properties.length == 1) {
			type = (ReadType) properties[0];
		} else {
			type = (ReadType) properties[0];
			interval = (Interval) properties[1];
		}
	}
	
	public void update(ReadType type) {
		if(this.type == type)
			if(this.type != ReadType.PE)
				count++;
			else 
				count+=2;
	}
	
	public void update(ReadType type, Interval interval) {
		if(this.type == type && this.interval == interval)
			count++;
	}
	
	public String formatKey() {
		String key = null;
		for(CounterProperty property : properties)
			key += property.toString();
		return key;
	}
	
	public long getReadsCount() {
		return count;
	}
}
