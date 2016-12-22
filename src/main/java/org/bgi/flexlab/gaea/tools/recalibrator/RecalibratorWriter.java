package org.bgi.flexlab.gaea.tools.recalibrator;

import org.bgi.flexlab.gaea.tools.recalibrator.table.RecalibratorTable;

public interface RecalibratorWriter {
	
	public void write(RecalibratorTable tables);
	
	public void close();
}
