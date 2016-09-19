
package org.bgi.flexlab.gaea.variantcontext.vcf;

import java.io.Serializable;
import java.util.Map;

import htsjdk.tribble.TribbleException;


/**

 *         <p/>
 *         Class VCFHeaderLine
 *         <p/>
 *         A class representing a key=value entry in the VCF header
 */
public class VCFHeaderLine implements Comparable<Object>, Serializable {
	private static final long serialVersionUID = -8797516130276433260L;
	protected static final boolean ALLOW_UNBOUND_DESCRIPTIONS = true;
    protected static final String UNBOUND_DESCRIPTION = "Not provided in original VCF header";

    private String mKey = null;
    private String mValue = null;


    /**
     * create a VCF header line
     *
     * @param key     the key for this header line
     * @param value   the value for this header line
     */
    public VCFHeaderLine(String key, String value) {
        if ( key == null ) {
            throw new IllegalArgumentException("VCFHeaderLine: key cannot be null");
        }
        mKey = key;
        mValue = value;
    }

    /**
     * Get the key
     *
     * @return the key
     */
    public String getKey() {
        return mKey;
    }

    /**
     * Get the value
     *
     * @return the value
     */
    public String getValue() {
        return mValue;
    }

    public String toString() {
        return toStringEncoding();
    }

    /**
     * Should be overloaded in sub classes to do subclass specific
     *
     * @return the string encoding
     */
    protected String toStringEncoding() {
        return mKey + "=" + mValue;
    }

    public boolean equals(Object o) {
        if ( !(o instanceof VCFHeaderLine) ) {
            return false;
        }
        return mKey.equals(((VCFHeaderLine)o).getKey()) && mValue.equals(((VCFHeaderLine)o).getValue());
    }

    public int compareTo(Object other) {
        return toString().compareTo(other.toString());
    }

    /**
     * @param line    the line
     * @return true if the line is a VCF meta data line, or false if it is not
     */
    public static boolean isHeaderLine(String line) {
        return line != null && line.length() > 0 && VCFHeader.HEADER_INDICATOR.equals(line.substring(0,1));
    }

    /**
     * create a string of a mapping pair for the target VCF version
     * @param keyValues a mapping of the key->value pairs to output
     * @return a string, correctly formatted
     */
    public static String toStringEncoding(Map<String, ? extends Object> keyValues) {
        StringBuilder builder = new StringBuilder();
        builder.append("<");
        boolean start = true;
        for (Map.Entry<String,?> entry : keyValues.entrySet()) {
            if (start) {
            	start = false;
            } else {
            	builder.append(",");
            }

            if ( entry.getValue() == null ) {
            	throw new TribbleException.InternalCodecException("Header problem: unbound value at " + entry + " from " + keyValues);
            }
            builder.append(entry.getKey());
            builder.append("=");
            builder.append(entry.getValue().toString().contains(",") ||
                           entry.getValue().toString().contains(" ") ||
                           entry.getKey().equals("Description") ? "\""+ entry.getValue() + "\"" : entry.getValue());
        }
        builder.append(">");
        return builder.toString();
    }
}