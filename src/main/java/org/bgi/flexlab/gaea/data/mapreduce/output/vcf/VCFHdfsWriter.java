package org.bgi.flexlab.gaea.data.mapreduce.output.vcf;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.bgi.flexlab.gaea.data.structure.vcf.VCFFileWriter;

import htsjdk.samtools.util.RuntimeEOFException;

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
		FileSystem fs;
		try {
			Path vcfPath = new Path(filePath);
			fs = vcfPath.getFileSystem(conf);
			os = new FSDataOutputStream(fs.create(vcfPath));
		} catch (IOException e) {
			throw new RuntimeEOFException(e);
		}
	}
}
