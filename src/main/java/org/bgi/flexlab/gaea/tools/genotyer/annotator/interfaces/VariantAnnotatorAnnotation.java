
package org.bgi.flexlab.gaea.tools.genotyer.annotator.interfaces;


import htsjdk.variant.vcf.VCFHeaderLine;

import java.util.List;
import java.util.Set;

public abstract class VariantAnnotatorAnnotation {
    // return the INFO keys
    public abstract List<String> getKeyNames();

    // initialization method (optional for subclasses, and therefore non-abstract)
    public void initialize ( Set<VCFHeaderLine> headerLines ) { }
}