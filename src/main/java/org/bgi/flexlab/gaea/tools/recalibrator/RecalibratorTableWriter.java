package org.bgi.flexlab.gaea.tools.recalibrator;

public abstract class RecalibratorTableWriter {
	
	public abstract void write(RecalibratorTable table);
	
	public abstract void close();
}
