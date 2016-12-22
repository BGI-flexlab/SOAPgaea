package org.bgi.flexlab.gaea.data.mapreduce.output.vcf;

import java.io.BufferedOutputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.bgi.flexlab.gaea.data.mapreduce.util.HdfsFileManager;
import org.bgi.flexlab.gaea.data.structure.vcf.VCFFileWriter;

public class VCFHdfsWriter extends VCFFileWriter{
	/**
	 * 
	 */
	private static final long serialVersionUID = 4557833627590736290L;

	public VCFHdfsWriter(String filePath, boolean doNotWriteGenotypes,
			final boolean allowMissingFieldsInHeader, Configuration conf) throws IOException {
		super(filePath, doNotWriteGenotypes, allowMissingFieldsInHeader, conf);
	}
	
	@Override
	public void initOutputStream(String filePath, Configuration conf) {
		os = new BufferedOutputStream(HdfsFileManager.getOutputStream(new Path(filePath), conf));
	}
}
