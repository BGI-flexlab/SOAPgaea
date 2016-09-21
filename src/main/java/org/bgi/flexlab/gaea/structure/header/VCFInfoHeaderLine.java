package org.bgi.flexlab.gaea.structure.header;


/**

 *         Class VCFInfoHeaderLine
 *         <p/>
 *         A class representing a key=value entry for INFO fields in the VCF header
 */
public class VCFInfoHeaderLine extends VCFCompoundHeaderLine {
	private static final long serialVersionUID = -7035639181083791002L;

	public VCFInfoHeaderLine(String name, int count, VCFHeaderLineType type, String description) {
        super(name, count, type, description, SupportedHeaderLineType.INFO);
    }

    public VCFInfoHeaderLine(String name, VCFHeaderLineCount count, VCFHeaderLineType type, String description) {
        super(name, count, type, description, SupportedHeaderLineType.INFO);
    }

    public VCFInfoHeaderLine(String line, VCFHeaderVersion version) {
        super(line, version, SupportedHeaderLineType.INFO);
    }

    // info fields allow flag values
    @Override
    boolean allowFlagValues() {
        return true;
    }
}
