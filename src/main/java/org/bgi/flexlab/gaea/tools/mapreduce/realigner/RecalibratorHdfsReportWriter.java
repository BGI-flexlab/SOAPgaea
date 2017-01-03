package org.bgi.flexlab.gaea.tools.mapreduce.realigner;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.bgi.flexlab.gaea.data.mapreduce.util.HdfsFileManager;
import org.bgi.flexlab.gaea.tools.recalibrator.report.RecalibratorReportTable;
import org.bgi.flexlab.gaea.tools.recalibrator.report.RecalibratorReportWriter;

public class RecalibratorHdfsReportWriter implements RecalibratorReportWriter{
	private FSDataOutputStream stream = null;
	
	public RecalibratorHdfsReportWriter(String out){
		Path path = new Path(out);
		Configuration conf = new Configuration();
		
		stream = HdfsFileManager.getOutputStream(path, conf);
	}

	@Override
	public void write(RecalibratorReportTable table) {
		try {
			stream.writeUTF(table.toString());
		} catch (IOException e) {
			throw new RuntimeException(e.toString());
		}
	}

	@Override
	public void close() {
		try {
			stream.close();
		} catch (IOException e) {
			throw new RuntimeException(e.toString());
		}
	}
}
