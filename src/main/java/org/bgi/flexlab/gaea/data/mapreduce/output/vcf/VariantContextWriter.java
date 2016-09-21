package org.bgi.flexlab.gaea.data.mapreduce.output.vcf;


import org.bgi.flexlab.gaea.data.structure.header.VCFHeader;

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
