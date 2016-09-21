package org.bgi.flexlab.gaea.outputformat.vcf;

import org.bgi.flexlab.gaea.structure.header.VCFHeader;

import htsjdk.variant.variantcontext.VariantContext;


/**
 * this class writes VCF files
 */
public interface VariantContextWriter {
    public void writeHeader(VCFHeader header);

    /**
     * attempt to close the VCF file
     */
    public void close();

    public void add(VariantContext vc);
}
