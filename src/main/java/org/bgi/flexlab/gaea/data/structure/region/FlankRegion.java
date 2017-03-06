package org.bgi.flexlab.gaea.data.structure.region;

public class FlankRegion extends Region {
	
	/**
	 * flank extend size
	 */
	private static int extendSize = 200;
	
	public FlankRegion() {
		super();
	}
	
	public void setRegionSize(int regionSize) {
		this.regionSize= regionSize + extendSize * 2;
	}
	
	public int getExtendSize() {
		return extendSize;
	}
	
}
