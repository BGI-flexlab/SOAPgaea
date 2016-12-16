package org.bgi.flexlab.gaea.data.structure.bam.clipper;

public class ClippingRegion {
	/**
	 * contains start position
	 */
	private final int start;
	
	/**
	 * not contains stop position
	 */
	private final int stop;

	public ClippingRegion(int start, int stop) {
		this.start = start;
		this.stop = stop;
	}

	public int getStart() {
		return start;
	}

	public int getStop() {
		return stop;
	}

	public int getLength() {
		return stop - start;
	}
}
