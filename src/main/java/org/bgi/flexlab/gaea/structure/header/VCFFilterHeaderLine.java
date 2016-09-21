package org.bgi.flexlab.gaea.structure.header;

import java.util.Arrays;

/**
 * 
 * A class representing a key=value entry for FILTER fields in the VCF header
 */
public class VCFFilterHeaderLine extends VCFSimpleHeaderLine  {

    /**
	 * 
	 */
	private static final long serialVersionUID = -7305808258914798991L;

	/**
     * create a VCF filter header line
     *
     * @param name         the name for this header line
     * @param description  the description for this header line
     */
    public VCFFilterHeaderLine(String name, String description) {
        super("FILTER", name, description);
    }

    /**
     * Convenience constructor for FILTER whose description is the name
     * @param name
     */
    public VCFFilterHeaderLine(String name) {
        super("FILTER", name, name);
    }

    /**
     * create a VCF info header line
     *
     * @param line      the header line
     * @param version   the vcf header version
     */
    public VCFFilterHeaderLine(String line, VCFHeaderVersion version) {
        super(line, version, "FILTER", Arrays.asList("ID", "Description"));
    }
}