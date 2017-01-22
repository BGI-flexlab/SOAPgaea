package org.bgi.flexlab.gaea.tools.bamqualtiycontrol.report;

import org.bgi.flexlab.gaea.tools.bamqualtiycontrol.report.CounterProperty.BaseType;
import org.bgi.flexlab.gaea.tools.bamqualtiycontrol.report.CounterProperty.Depth;
import org.bgi.flexlab.gaea.tools.bamqualtiycontrol.report.CounterProperty.DepthType;
import org.bgi.flexlab.gaea.tools.bamqualtiycontrol.report.CounterProperty.Interval;

public class BaseCounter {

	private long baseCount;
	
	private long baseWithoutPCRdupCount;
	
	private long totalDepth;
		
	private Interval region;
	
	private Depth depth;
	
	private DepthType dType;
	
	private BaseType bType;
	
	private CounterProperty[] properties;
	
	public BaseCounter(CounterProperty... counterProperties) {
//		if counter properties has 3 elements, this counter is working for the region report
		properties = counterProperties;
		if(properties.length == 3) {
			region = (Interval)properties[0];
			depth = (Depth)properties[1];
			dType = (DepthType)properties[2];
			bType = null;
		}
//		if counter properties has 1 element, this counter is working for the basic report
		if(properties.length == 1) {
			bType = (BaseType) properties[0];
		}
	}
	
	public void update(BaseType bType) {
		if(this.bType == bType)
			this.baseCount += bType.getCount();
	}
	
	public void update(Interval region, DepthType depth, DepthType noPCRdepth) {
		// TODO Auto-generated method stub
		if(this.region == region)
			switch (dType) {
				case NORMAL:
					update(depth);
					break;
				case WITHOUT_PCR:
					update(noPCRdepth);
					break;
				default:
					break;
			}
	}

	private void update(DepthType depth) {
		if(depth.getDepth() > this.depth.getDepth() && this.depth != Depth.TOTALDEPTH)
			baseCount++;
		else if(this.depth == Depth.TOTALDEPTH)
			totalDepth += depth.getDepth();
		else if(this.depth == Depth.ABOVE_ZREO && depth.getDepth() == 0) {
			baseCount++;
			baseWithoutPCRdupCount++;
		}
	}

	public long getBaseCount() {
		return baseCount;
	}
	
	public long getBaseWithoutPCRDupCount() {
		return baseWithoutPCRdupCount;
	}

	public long getTotalDepth() {
		// TODO Auto-generated method stub
		return totalDepth;
	}
	
	public long getProperty() {
		long result = 0;
		switch (depth) {
		case TOTALDEPTH:
			result = totalDepth;
		default:
			switch (dType) {
			case NORMAL:
				result = baseCount;
			case WITHOUT_PCR:
				result = baseWithoutPCRdupCount;
			}
		}
		return result;
	}
	
	public String formatKey() {
		String key = null;
		for(CounterProperty property : properties)
			key += property.toString();
		return key;
	}
	
	public Interval interval() {
		return region;
	}
	
	public Depth depth() {
		return depth;
	}
	
	public DepthType type() {
		return dType;
	}

}
