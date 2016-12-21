package org.bgi.flexlab.gaea.tools.recalibrator;

public interface RecalibratorWriter {
	
	public void write(RecalibratorTable tables);
	
	public void close();
}
