package org.bgi.flexlab.gaea.tools.recalibrator.report;

public interface RecalibratorReportWriter {
	public void write(RecalibratorReportTable table);
	
	public void close();
}
