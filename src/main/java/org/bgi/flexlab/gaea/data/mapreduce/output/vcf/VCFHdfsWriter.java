package org.bgi.flexlab.gaea.data.mapreduce.output.vcf;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import htsjdk.samtools.util.RuntimeEOFException;

public class VCFHdfsWriter extends VCFFileWriter{
	public VCFHdfsWriter(String filePath, boolean doNotWriteGenotypes,
			final boolean allowMissingFieldsInHeader, Configuration conf) {
		super(filePath, doNotWriteGenotypes, allowMissingFieldsInHeader);
	}
	
	@Override
	public void initWriter(String filePath, Configuration conf) {
		FileSystem fs;
		try {
			Path vcfPath = new Path(filePath);
			fs = vcfPath.getFileSystem(conf);
			writer = new BufferedWriter(new OutputStreamWriter(fs.create(vcfPath)));
		} catch (IOException e) {
			throw new RuntimeEOFException(e);
		}
	}
}
