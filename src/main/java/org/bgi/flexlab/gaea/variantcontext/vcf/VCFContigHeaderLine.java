
package org.bgi.flexlab.gaea.variantcontext.vcf;

import java.util.Map;

/**
 * A special class representing a contig VCF header line.  Nows the true contig order and sorts on that
 *
 * @author mdepristo
 */
public class VCFContigHeaderLine extends VCFSimpleHeaderLine {
    /**
	 * 
	 */
	private static final long serialVersionUID = 4696770249614606640L;
	final Integer contigIndex;


    /**
     * create a VCF contig header line
     *
     * @param line      the header line
     * @param version   the vcf header version
     * @param key            the key for this header line
     */
    public VCFContigHeaderLine(final String line, final VCFHeaderVersion version, final String key, int contigIndex) {
        super(line, version, key, null);
        this.contigIndex = contigIndex;
    }

    public VCFContigHeaderLine(final Map<String, String> mapping, int contigIndex) {
        super(VCFHeader.CONTIG_KEY, mapping, null);
        this.contigIndex = contigIndex;
    }

    public Integer getContigIndex() {
        return contigIndex;
    }

    /**
     * IT IS CRITIAL THAT THIS BE OVERRIDDEN SO WE SORT THE CONTIGS IN THE CORRECT ORDER
     *
     * @param other
     * @return
     */
    @Override
    public int compareTo(final Object other) {
        if ( other instanceof VCFContigHeaderLine )
            return contigIndex.compareTo(((VCFContigHeaderLine) other).contigIndex);
        else {
            return super.compareTo(other);
        }
    }
}