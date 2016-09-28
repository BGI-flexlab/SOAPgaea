package org.bgi.flexlab.gaea.data.structure.vcf;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.writer.Options;
import htsjdk.variant.variantcontext.writer.VariantContextWriter;
import htsjdk.variant.variantcontext.writer.VariantContextWriterBuilder;
import htsjdk.variant.vcf.VCFHeader;

public abstract class VCFFileWriter implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -2686163989598391782L;
	
	private VariantContextWriterBuilder builder;
	
	private VariantContextWriter writer;
		
	private boolean hasWrittenHeader = false;
	
	protected OutputStream os;
		
	public VCFFileWriter(String path,boolean doNotWriteGenotypes,
			final boolean allowMissingFieldsInHeader, Configuration conf) throws IOException {
		initOutputStream(path, conf);
		initBuilder(os, doNotWriteGenotypes, allowMissingFieldsInHeader);
		this.writer = builder.build();
	}
	
	public abstract void initOutputStream(String path, Configuration conf); 
	
	private void initBuilder(OutputStream os, boolean doNotWriteGenotypes, 
			boolean allowMissingFieldsInHeader) {
		builder = new VariantContextWriterBuilder();
		builder.clearOptions();
		if(doNotWriteGenotypes) {
			builder.setOption(Options.DO_NOT_WRITE_GENOTYPES);
		}
		if(allowMissingFieldsInHeader) {
			builder.setOption(Options.ALLOW_MISSING_FIELDS_IN_HEADER);
		}
		builder.setOutputStream(os);
	}
	
	public void writeHeader(VCFHeader header) {
		writer.writeHeader(header);
		hasWrittenHeader = true;
	}
	
	public void close() {
		writer.close();
	}
	
	public void add(VariantContext vc) {
		if (!hasWrittenHeader) {
			throw new IllegalStateException(
					"The VCF Header must be written before records can be added: ");
		}
		writer.add(vc);
	}
}
