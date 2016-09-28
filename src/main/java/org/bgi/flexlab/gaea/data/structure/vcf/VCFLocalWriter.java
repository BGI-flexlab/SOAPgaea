package org.bgi.flexlab.gaea.data.structure.vcf;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;

import htsjdk.samtools.util.RuntimeEOFException;

public class VCFLocalWriter extends VCFFileWriter {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 3915150472678717360L;

	public VCFLocalWriter(String filePath, boolean doNotWriteGenotypes,
			final boolean allowMissingFieldsInHeader) throws IOException {
		super(filePath, doNotWriteGenotypes, allowMissingFieldsInHeader, null);
	}

	@Override
	public void initOutputStream(String filePath, Configuration conf) {
		// TODO Auto-generated method stub
		try {
			os = new FileOutputStream(new File(filePath));
		} catch (IOException e) {
			throw new RuntimeEOFException(e);
		}
	}
}
