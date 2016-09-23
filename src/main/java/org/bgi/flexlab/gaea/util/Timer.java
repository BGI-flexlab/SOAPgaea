
package org.bgi.flexlab.gaea.util;

public class Timer {
	private long t0, td;

	public long start () { return t0 = System.nanoTime(); }
	public long stopNs() { return td = System.nanoTime() - t0; }
	public long stopS () { return stopNs() / 1000000000L; }

	public int fms() { return (int)(td / 1000000 % 1000); }
}
