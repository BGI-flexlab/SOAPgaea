package org.bgi.flexlab.gaea.data.structure.vcf.writer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.bgi.flexlab.gaea.data.mapreduce.output.vcf.VCFFileWriter;

import htsjdk.samtools.util.RuntimeEOFException;

public class VCFLocalWriter extends VCFFileWriter {
	
	public VCFLocalWriter(String filePath, boolean doNotWriteGenotypes,
			final boolean allowMissingFieldsInHeader) {
		super(filePath, doNotWriteGenotypes, allowMissingFieldsInHeader);
	}

	@Override
	public void initWriter(String filePath, Configuration conf) {
		// TODO Auto-generated method stub
		try {
			writer = new BufferedWriter(new FileWriter(new File(filePath)));
		} catch (IOException e) {
			throw new RuntimeEOFException(e);
		}
	}
}
